use std::collections::HashMap;
use std::sync::Arc;

use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use uuid::Uuid;

use serde::{Deserialize, Serialize};

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Message, MessageType, Reply};

use crate::capability::{RpcCapability, negotiation};
use crate::service::RpcService;
use crate::stream::{
    EncryptedRpcReceiver, EncryptedRpcSender, RpcAsyncReceiver, RpcAsyncSender, RpcReceiver,
    RpcSender,
};

use crate::sync::DynamicLatch;
use crate::transport::TransportLayer;

enum Response {
    Pong,
    Reply(Reply),
}

struct ClientState<S, H> {
    abort_lock: DynamicLatch,
    pending: Mutex<HashMap<Uuid, Sender<RpcResult<Response>>>>,
    sender: Mutex<S>,
    service: H,
}

impl<S, H> ClientState<S, H> {
    #[inline(always)]
    fn new(service: H, sender: S, cap: usize) -> ClientState<S, H> {
        ClientState {
            abort_lock: DynamicLatch::new(),
            pending: Mutex::const_new(HashMap::with_capacity(cap)),
            sender: Mutex::const_new(sender),
            service,
        }
    }
}

/// RPC Client implementation.
/// This implementation utilizes single shared transport instance,
/// which makes it very lightweight at the cost of some synchronization overhead.
pub struct RpcClient<S, H> {
    state: Arc<ClientState<S, H>>,
    task: JoinHandle<()>,
}

impl<S, H> RpcClient<S, H>
where
    S: RpcAsyncSender + 'static,
    H: RpcService + Send + Sync + 'static,
{
    #[inline]
    pub async fn connect<T>(
        mut transport: T,
        call_handler: H,
        capacity: usize,
    ) -> RpcResult<RpcClient<RpcSender<T::OwnedWriteHalf>, H>>
    where
        T: TransportLayer + 'static,
    {
        negotiation::initiate(&mut transport, RpcCapability::new(1, false)).await?;

        let (r, w) = transport.into_split();
        Self::connect_with_parts(
            RpcReceiver::new(r),
            RpcSender::new(w),
            call_handler,
            capacity,
        )
        .await
    }

    #[inline]
    pub async fn connect_encrypted<T>(
        mut transport: T,
        call_handler: H,
        capacity: usize,
    ) -> RpcResult<RpcClient<EncryptedRpcSender<T::OwnedWriteHalf>, H>>
    where
        T: TransportLayer + 'static,
    {
        negotiation::initiate(&mut transport, RpcCapability::new(1, true)).await?;

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut transport).await?;

        let (r, w) = transport.into_split();

        Self::connect_with_parts(
            EncryptedRpcReceiver::new(r, r_key),
            EncryptedRpcSender::new(w, w_key),
            call_handler,
            capacity,
        )
        .await
    }

    #[inline(always)]
    const fn new(state: Arc<ClientState<S, H>>, task: tokio::task::JoinHandle<()>) -> Self {
        Self { state, task }
    }

    async fn connect_with_parts<R, W>(
        mut rx: R,
        tx: W,
        call_handler: H,
        cap: usize,
    ) -> RpcResult<RpcClient<W, H>>
    where
        R: RpcAsyncReceiver + Send + 'static,
        W: RpcAsyncSender + Send + 'static,
    {
        let state = Arc::new(ClientState::new(call_handler, tx, cap));
        let c_state = Arc::clone(&state);

        let task = tokio::spawn(async move {
            loop {
                match rx.receive().await {
                    Ok(message) => {
                        if let Err(e) = Self::process_incoming(&c_state, message).await {
                            eprintln!("Handling error: {e}");
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Receiving error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(RpcClient::new(state, task))
    }

    async fn process_incoming<W>(state: &ClientState<W, H>, message: Message) -> RpcResult<()>
    where
        W: RpcAsyncSender + 'static,
    {
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
                    match state.service.call(&call).await {
                        Ok(reply) => {
                            let message = Message::reply(message.id, reply);
                            return state.sender.lock().await.send(&message).await;
                        }
                        Err(e) => {
                            let message = Message::error(message.id, e);
                            return state.sender.lock().await.send(&message).await;
                        }
                    }
                }
            }
            MessageType::Ping => {
                let pong = Message::pong(message.id);
                return state.sender.lock().await.send(&pong).await;
            }
            _ => {
                eprintln!("Received unexpected message type: {:?}", message.kind);
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

    /// Makes a remote procedure call.
    /// Default timeout is `30` seconds.
    pub async fn call<P, R>(&self, method: u16, params: P) -> RpcResult<R>
    where
        P: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        self.call_with_timeout(method, params, Duration::from_secs(30))
            .await
    }

    /// Makes a remote procedure call with custom timeout.
    pub async fn call_with_timeout<P, R>(
        &self,
        method: u16,
        params: P,
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
                let result: R = Message::decode_as(&reply.data)?;
                Ok(result)
            }
            _ => Err(RpcError::error(ErrKind::UnexpectedMsg)),
        }
    }

    /// Sends a notification without response.
    pub async fn notify<P>(&self, method: u16, params: P) -> RpcResult<()>
    where
        P: Serialize,
    {
        let message = Message::notification_with(method, params)?;
        self.state.sender.lock().await.send(&message).await
    }

    /// Sends a `ping`` message.
    pub async fn ping(&self, timeout: Duration) -> RpcResult<()> {
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
    pub async fn shutdown(&mut self) -> RpcResult<()> {
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

    use crate::message::{Call, Reply};

    #[tokio::test]
    async fn test_client_call_reply() {
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
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Call(call) => {
                                assert_eq!(call.method, 1);
                                let params: String = Message::decode_as(&call.data).unwrap();
                                assert_eq!(params, "call".to_string());

                                // Send a response.
                                let response =
                                    Message::reply_with(message.id, "reply".to_string()).unwrap();
                                let _ = rpc_writer.send(&response).await;
                            }
                            _ => panic!("Expected call"),
                        }
                        break;
                    }
                    Err(e) => {
                        println!("Server error: {e}");
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>, ()>::connect(transport, (), 1)
            .await
            .unwrap();

        let reply = client.call::<&str, String>(1, "call").await.unwrap();
        assert_eq!(reply, "reply".to_string());

        server_task.await.unwrap();
        client.shutdown().await.unwrap();
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

            loop {
                match rpc_reader.receive().await {
                    Ok(message) => match &message.kind {
                        MessageType::Call(call) => {
                            assert_eq!(call.method, 1);
                            let params: String = Message::decode_as(&call.data).unwrap();
                            assert_eq!(params, "call");

                            let response =
                                Message::reply_with(message.id, "reply".to_string()).unwrap();
                            let _ = rpc_writer.send(&response).await;
                            break;
                        }
                        _ => panic!("Expected call message"),
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
            RpcClient::<EncryptedRpcSender<tcp::OwnedWriteHalf>, ()>::connect_encrypted(
                transport,
                (),
                1,
            )
            .await
            .unwrap();

        let reply = client.call::<&str, String>(1, "call").await.unwrap();
        assert_eq!(reply, "reply".to_string());

        server_task.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_client_error_response() {
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
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Call(call) => {
                                assert_eq!(call.method, 2);
                                let error = Message::error(
                                    message.id,
                                    RpcError::error(ErrKind::Unimplemented),
                                );
                                let _ = rpc_writer.send(&error).await;
                            }
                            _ => panic!("Expected call"),
                        }
                        break;
                    }
                    Err(e) => {
                        println!("Server error: {e}");
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>, ()>::connect(transport, (), 1)
            .await
            .unwrap();

        let result: Result<(), RpcError> = client.call(2, "call").await;

        match result {
            Ok(_) => panic!("Expected error result"),
            Err(e) => {
                assert!(e.kind == ErrKind::Unimplemented);
            }
        }

        server_task.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_notification() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let received_notification = Arc::new(tokio::sync::Mutex::new(false));
        let received_clone = Arc::clone(&received_notification);

        let server_task = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.unwrap();

            negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            let (r, _) = transport.into_split();
            let mut rpc_reader = RpcReceiver::new(r);

            loop {
                match rpc_reader.receive().await {
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Notification(n) => {
                                assert_eq!(n.method, 1);
                                let message: String = Message::decode_as(&n.data).unwrap();
                                assert_eq!(message, "some event");
                                *received_clone.lock().await = true;
                            }
                            _ => panic!("Expected notification"),
                        }
                        break;
                    }
                    Err(e) => {
                        println!("Server error: {e}");
                        break;
                    }
                }
            }
        });

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>, ()>::connect(transport, (), 0)
            .await
            .unwrap();

        client.notify(1, "some event").await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(*received_notification.lock().await);

        server_task.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[derive(Clone)]
    struct ClientHandler {}

    impl RpcService for ClientHandler {
        async fn call(&self, call: &Call) -> RpcResult<Reply> {
            match call.method {
                1 => {
                    let src = call.decode_as::<String>().unwrap();
                    Ok(Reply::with(&format!("Reply to {src}")).unwrap())
                }
                _ => Err(RpcError::error(ErrKind::Unimplemented)),
            }
        }
    }

    #[tokio::test]
    async fn test_client_bidirectional_call() {
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

            let server_call = Message::call_with(1, "C1").unwrap();
            let _ = rpc_writer.send(&server_call).await;

            loop {
                match rpc_reader.receive().await {
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Reply(reply) => {
                                assert_eq!(Some(message.id), Some(server_call.id));
                                let response: String = Message::decode_as(&reply.data).unwrap();
                                assert!(&response == "Reply to C1");
                            }
                            _ => panic!("Expected reply"),
                        }
                        break;
                    }
                    Err(e) => {
                        println!("Server error: {e}");
                        break;
                    }
                }
            }
        });

        let handler = ClientHandler {};

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>, ClientHandler>::connect(
            transport, handler, 0,
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        server_task.await.unwrap();
        client.shutdown().await.unwrap()
    }
}
