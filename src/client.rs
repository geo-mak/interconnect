use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use serde::{Deserialize, Serialize};

use uuid::Uuid;

use crate::capability::{RpcCapability, negotiation};
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Call, Message, MessageType, Reply};
use crate::report::Reporter;
use crate::service::{CallContext, RpcService};
use crate::stream::{
    EncryptedRpcReceiver, EncryptedRpcSender, RpcAsyncReceiver, RpcAsyncSender, RpcReceiver,
    RpcSender,
};
use crate::sync::DynamicLatch;
use crate::transport::TransportLayer;

/// The common RPC client interface of async clients.
pub trait AsyncRpcClient {
    fn call<P, R>(&self, method: u16, params: &P) -> impl Future<Output = RpcResult<R>>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>;

    fn call_with_timeout<P, R>(
        &self,
        method: u16,
        params: &P,
        timeout: Duration,
    ) -> impl Future<Output = RpcResult<R>>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>;

    fn call_one_way<P>(&self, method: u16, params: &P) -> impl Future<Output = RpcResult<()>>
    where
        P: Serialize;

    fn nullary_call<R>(&self, method: u16) -> impl Future<Output = RpcResult<R>>
    where
        R: for<'de> Deserialize<'de>;

    fn nullary_call_with_timeout<R>(
        &self,
        method: u16,
        timeout: Duration,
    ) -> impl Future<Output = RpcResult<R>>
    where
        R: for<'de> Deserialize<'de>;

    fn nullary_call_one_way(&self, method: u16) -> impl Future<Output = RpcResult<()>>;

    fn ping(&self, timeout: Duration) -> impl Future<Output = RpcResult<()>>;

    fn shutdown(&mut self) -> impl Future<Output = RpcResult<()>>;
}

enum Response {
    Pong,
    Reply(Reply),
}

struct ClientState<S, H, E> {
    reporter: E,
    abort_lock: DynamicLatch,
    pending: Mutex<HashMap<Uuid, Sender<RpcResult<Response>>>>,
    sender: Mutex<S>,
    service: H,
}

impl<S, H, E> ClientState<S, H, E> {
    #[inline(always)]
    fn new(service: H, sender: S, cap: usize, reporter: E) -> ClientState<S, H, E> {
        ClientState {
            reporter,
            abort_lock: DynamicLatch::new(),
            pending: Mutex::const_new(HashMap::with_capacity(cap)),
            sender: Mutex::const_new(sender),
            service,
        }
    }
}

struct ClientContext<'a, S, H, E> {
    id: &'a Uuid,
    state: &'a ClientState<S, H, E>,
}

impl<'a, S, H, E> ClientContext<'a, S, H, E>
where
    S: RpcAsyncSender + Send,
{
    #[inline(always)]
    const fn new(id: &'a Uuid, sender: &'a ClientState<S, H, E>) -> Self {
        Self { id, state: sender }
    }
}

impl<'a, S, H, E> CallContext for ClientContext<'a, S, H, E>
where
    S: RpcAsyncSender + Send,
    H: RpcService + Sync,
    E: Reporter + Sync,
{
    type ID = Uuid;

    #[inline(always)]
    fn id(&self) -> &Self::ID {
        self.id
    }

    #[inline]
    async fn send_reply(&mut self, reply: Reply) -> RpcResult<()> {
        let message = Message::reply(*self.id, reply);
        self.state.sender.lock().await.send(&message).await
    }

    #[inline]
    async fn send_error(&mut self, err: RpcError) -> RpcResult<()> {
        let message = Message::error(*self.id, err);
        self.state.sender.lock().await.send(&message).await
    }

    #[inline]
    async fn call(&mut self, call: Call) -> RpcResult<()> {
        let message = Message::new(*self.id, MessageType::Call(call));
        self.state.sender.lock().await.send(&message).await
    }

    #[inline]
    async fn nullary_call(&mut self, method: u16) -> RpcResult<()> {
        let message = Message::new(*self.id, MessageType::NullaryCall(method));
        self.state.sender.lock().await.send(&message).await
    }
}

/// An async RPC client implementation.
/// This implementation utilizes single shared transport instance,
/// which makes it very lightweight at the cost of some synchronization overhead.
pub struct RpcAsyncClient<S, H, E> {
    state: Arc<ClientState<S, H, E>>,
    task: JoinHandle<()>,
}

impl<S, H, E> RpcAsyncClient<S, H, E>
where
    S: RpcAsyncSender + Send + 'static,
    H: RpcService + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    #[inline]
    pub async fn connect<T>(
        mut transport: T,
        call_handler: H,
        capacity: usize,
        reporter: E,
    ) -> RpcResult<RpcAsyncClient<RpcSender<T::OwnedWriteHalf>, H, E>>
    where
        T: TransportLayer + 'static,
    {
        negotiation::initiate(&mut transport, RpcCapability::new(1, false)).await?;

        let (r, w) = transport.into_split();
        RpcAsyncClient::connect_with_parts(
            RpcSender::new(w),
            RpcReceiver::new(r),
            call_handler,
            capacity,
            reporter,
        )
        .await
    }

    #[inline]
    pub async fn connect_encrypted<T>(
        mut transport: T,
        call_handler: H,
        capacity: usize,
        reporter: E,
    ) -> RpcResult<RpcAsyncClient<EncryptedRpcSender<T::OwnedWriteHalf>, H, E>>
    where
        T: TransportLayer + 'static,
    {
        negotiation::initiate(&mut transport, RpcCapability::new(1, true)).await?;

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut transport).await?;

        let (r, w) = transport.into_split();

        RpcAsyncClient::connect_with_parts(
            EncryptedRpcSender::new(w, w_key),
            EncryptedRpcReceiver::new(r, r_key),
            call_handler,
            capacity,
            reporter,
        )
        .await
    }

    #[inline(always)]
    const fn new(state: Arc<ClientState<S, H, E>>, task: tokio::task::JoinHandle<()>) -> Self {
        Self { state, task }
    }

    async fn connect_with_parts<R>(
        sender: S,
        mut receiver: R,
        call_handler: H,
        cap: usize,
        reporter: E,
    ) -> RpcResult<RpcAsyncClient<S, H, E>>
    where
        S: RpcAsyncSender + Send + 'static,
        R: RpcAsyncReceiver + Send + 'static,
    {
        let state = Arc::new(ClientState::new(call_handler, sender, cap, reporter));
        let c_state = Arc::clone(&state);

        let task = tokio::spawn(async move {
            loop {
                match receiver.receive().await {
                    Ok(message) => {
                        if let Err(e) = Self::process_message(&c_state, message).await {
                            c_state.reporter.error("Handling error", &e);
                            break;
                        }
                    }
                    Err(e) => {
                        c_state.reporter.error("Receiving error", &e);
                        break;
                    }
                }
            }
        });

        Ok(RpcAsyncClient::new(state, task))
    }

    async fn process_message(state: &Arc<ClientState<S, H, E>>, message: Message) -> RpcResult<()> {
        match message.kind {
            MessageType::Reply(reply) => {
                let mut pending = state.pending.lock().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Ok(Response::Reply(reply)));
                }
            }
            MessageType::Error(err) => {
                let mut pending = state.pending.lock().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Err(err));
                }
            }
            MessageType::Pong => {
                let mut pending = state.pending.lock().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Ok(Response::Pong));
                }
            }
            MessageType::Call(call) => {
                if let Some(_lock) = state.abort_lock.acquire() {
                    let mut ctx = ClientContext::new(&message.id, state);
                    return state.service.call(&call, &mut ctx).await;
                }
            }
            MessageType::NullaryCall(method) => {
                if let Some(_lock) = state.abort_lock.acquire() {
                    let mut ctx = ClientContext::new(&message.id, state);
                    return state.service.nullary_call(method, &mut ctx).await;
                }
            }
            MessageType::Ping => {
                let pong = Message::pong(message.id);
                return state.sender.lock().await.send(&pong).await;
            }
        }
        Ok(())
    }

    /// Sends a message and waits for response.
    async fn send_message(
        &self,
        message: &Message,
        timeout_duration: Duration,
    ) -> RpcResult<Response> {
        let (sender, receiver) = oneshot::channel();

        let id = message.id;

        // Store pending request.
        {
            let mut pending = self.state.pending.lock().await;
            pending.insert(id, sender);
        }

        // Send the message.
        if let Err(e) = self.state.sender.lock().await.send(message).await {
            let mut pending = self.state.pending.lock().await;
            pending.remove(&id);
            return Err(e);
        }

        // Wait for response with timeout.
        match timeout(timeout_duration, receiver).await {
            Ok(Ok(result)) => result,
            // Timeout occurred.
            Err(_) => {
                let mut pending = self.state.pending.lock().await;
                pending.remove(&id);
                Err(RpcError::error(ErrKind::Timeout))
            }
            _ => unreachable!(),
        }
    }
}

impl<S, H, E> AsyncRpcClient for RpcAsyncClient<S, H, E>
where
    S: RpcAsyncSender + Send + 'static,
    H: RpcService + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    /// Makes a remote procedure call.
    /// Default timeout is `30` seconds.
    async fn call<P, R>(&self, method: u16, params: &P) -> RpcResult<R>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        self.call_with_timeout(method, params, Duration::from_secs(30))
            .await
    }

    /// Makes a remote procedure call with custom timeout.
    async fn call_with_timeout<P, R>(
        &self,
        method: u16,
        params: &P,
        timeout: Duration,
    ) -> RpcResult<R>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let message = Message::call_with(method, params)?;

        let response = self.send_message(&message, timeout).await?;

        match response {
            Response::Reply(reply) => {
                let result: R = Message::decode_from_slice(&reply.data)?;
                Ok(result)
            }
            _ => Err(RpcError::error(ErrKind::UnexpectedMsg)),
        }
    }

    /// Sends a one-way call without response.
    ///
    /// This call is untracked, if the target method returns response,
    /// the response will be discarded.
    async fn call_one_way<P>(&self, method: u16, params: &P) -> RpcResult<()>
    where
        P: Serialize,
    {
        let message = Message::call_with(method, params)?;
        self.state.sender.lock().await.send(&message).await
    }

    /// Makes a remote procedure call.
    /// Default timeout is `30` seconds.
    async fn nullary_call<R>(&self, method: u16) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        self.nullary_call_with_timeout(method, Duration::from_secs(30))
            .await
    }

    /// Makes a remote procedure call with custom timeout.
    async fn nullary_call_with_timeout<R>(&self, method: u16, timeout: Duration) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        let message = Message::nullary_call(method);
        let response = self.send_message(&message, timeout).await?;
        match response {
            Response::Reply(reply) => {
                let result: R = Message::decode_from_slice(&reply.data)?;
                Ok(result)
            }
            _ => Err(RpcError::error(ErrKind::UnexpectedMsg)),
        }
    }

    /// Sends a one-way nullary call without response.
    ///
    /// This call is untracked, if the target method returns response,
    /// the response will be discarded.
    async fn nullary_call_one_way(&self, method: u16) -> RpcResult<()> {
        let message = Message::nullary_call(method);
        self.state.sender.lock().await.send(&message).await
    }

    /// Sends a `ping`` message.
    async fn ping(&self, timeout: Duration) -> RpcResult<()> {
        let _ = self.send_message(&Message::ping(), timeout).await?;
        Ok(())
    }

    /// Closes its sender and shutdowns the receiving task in graceful manner.
    ///
    /// This call doesn't have immediate effect and may take longer time,
    /// because it allows critical regions to fully complete their execution.
    ///
    /// Buffered data will be sent followed by FIN message.
    ///
    /// Any attempts to send messages after this call will return `Broken pipe` I/O error.
    async fn shutdown(&mut self) -> RpcResult<()> {
        self.state.abort_lock.open();

        self.state.abort_lock.wait().await;

        self.task.abort();

        self.state.sender.lock().await.close().await?;

        self.state.service.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::net::{TcpStream, tcp};

    use crate::report::STDIOReporter;

    #[tokio::test]
    async fn test_client_calls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.unwrap();

            negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            let (r, w) = transport.into_split();
            let mut rpc_reader = RpcReceiver::new(r);
            let mut rpc_writer = RpcSender::new(w);

            loop {
                match rpc_reader.receive().await {
                    Ok(message) => match &message.kind {
                        MessageType::Call(call) => match call.method {
                            1 => {
                                let params: String =
                                    Message::decode_from_slice(&call.data).unwrap();
                                assert_eq!(params, "call");

                                let response = Message::reply_with(message.id, &"reply").unwrap();
                                let _ = rpc_writer.send(&response).await;
                            }
                            2 => {
                                let error = Message::error(
                                    message.id,
                                    RpcError::error(ErrKind::Unimplemented),
                                );
                                let _ = rpc_writer.send(&error).await;
                            }
                            _ => panic!("undefined method"),
                        },
                        MessageType::NullaryCall(method) => {
                            assert_eq!(*method, 1);
                            let response =
                                Message::reply_with(message.id, &"nullary call reply").unwrap();
                            let _ = rpc_writer.send(&response).await;
                        }
                        _ => panic!("Expected call"),
                    },
                    Err(e) => {
                        println!("Server error: {e}");
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client =
            RpcAsyncClient::<RpcSender<tcp::OwnedWriteHalf>, (), STDIOReporter>::connect(
                transport,
                (),
                1,
                STDIOReporter::new(),
            )
            .await
            .unwrap();

        let reply = client.call::<&str, String>(1, &"call").await.unwrap();
        assert_eq!(reply, "reply");

        let reply_nullary = client.nullary_call::<String>(1).await.unwrap();
        assert_eq!(reply_nullary, "nullary call reply");

        let err: RpcError = client.call::<&str, String>(2, &"call").await.unwrap_err();
        assert!(err.kind == ErrKind::Unimplemented);

        client.shutdown().await.unwrap();
        server_task.await.unwrap()
    }

    #[tokio::test]
    async fn test_client_encrypted_call_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.unwrap();

            let proposed = negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            assert!(proposed.encryption);

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            assert!(proposed.encryption);

            let (r_key, w_key) = negotiation::accept_key_exchange(&mut transport)
                .await
                .expect("Server encryption setup failed");

            let (r, w) = transport.into_split();
            let mut rpc_reader = EncryptedRpcReceiver::new(r, r_key);
            let mut rpc_writer = EncryptedRpcSender::new(w, w_key);

            match rpc_reader.receive().await {
                Ok(message) => match &message.kind {
                    MessageType::Call(call) => {
                        assert_eq!(call.method, 1);
                        let params: String = Message::decode_from_slice(&call.data).unwrap();
                        assert_eq!(params, "call");

                        let response = Message::reply_with(message.id, &"reply").unwrap();
                        let _ = rpc_writer.send(&response).await;
                    }
                    _ => panic!("Expected call message"),
                },
                Err(e) => {
                    println!("Server error: {e}");
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client =
            RpcAsyncClient::<EncryptedRpcSender<tcp::OwnedWriteHalf>, (), STDIOReporter>::connect_encrypted(
                transport,
                (),
                1,
                STDIOReporter::new()
            )
            .await
            .unwrap();

        let reply = client.call::<&str, String>(1, &"call").await.unwrap();
        assert_eq!(reply, "reply");

        server_task.await.unwrap();
        client.shutdown().await.unwrap();
    }
}
