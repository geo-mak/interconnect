use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::time::timeout;

use uuid::Uuid;

use serde::{Deserialize, Serialize};

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Message, MessageType};

use crate::capability::RpcCapability;
use crate::capability::negotiation;
use crate::service::RpcService;
use crate::transport::EncryptedRpcReceiver;
use crate::transport::EncryptedRpcSender;
use crate::transport::RpcAsyncReceiver;
use crate::transport::RpcAsyncSender;
use crate::transport::{OwnedSplitStream, RpcReceiver, RpcSender};

struct ClientState<T> {
    sender: Mutex<T>,
    pending: Mutex<HashMap<Uuid, Sender<RpcResult<MessageType>>>>,
}

impl<T> ClientState<T> {
    #[inline(always)]
    fn new(sender: T) -> ClientState<T> {
        ClientState {
            sender: Mutex::new(sender),
            pending: Mutex::new(HashMap::new()),
        }
    }
}

/// RPC Client implementation.
pub struct RpcClient<T> {
    state: Arc<ClientState<T>>,
    task: tokio::task::JoinHandle<()>,
}

impl<T> RpcClient<T>
where
    T: RpcAsyncSender + 'static,
{
    #[inline(always)]
    const fn new(state: Arc<ClientState<T>>, task: tokio::task::JoinHandle<()>) -> Self {
        Self { state, task }
    }

    #[inline]
    pub async fn connect<S, H>(
        mut io_stream: S,
        call_handler: H,
    ) -> RpcResult<RpcClient<RpcSender<S::OwnedWriteHalf>>>
    where
        S: OwnedSplitStream + 'static,
        H: RpcService,
    {
        negotiation::initiate_capability(&mut io_stream, RpcCapability::new(1, false)).await?;

        let (r, w) = io_stream.owned_split();
        Self::connect_with_parts(RpcReceiver::new(r), RpcSender::new(w), call_handler).await
    }

    #[inline]
    pub async fn connect_encrypted<S, H>(
        mut io_stream: S,
        call_handler: H,
    ) -> RpcResult<RpcClient<EncryptedRpcSender<S::OwnedWriteHalf>>>
    where
        S: OwnedSplitStream + 'static,
        H: RpcService,
    {
        negotiation::initiate_capability(&mut io_stream, RpcCapability::new(1, true)).await?;

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut io_stream).await?;

        let (r, w) = io_stream.owned_split();

        Self::connect_with_parts(
            EncryptedRpcReceiver::new(r, r_key),
            EncryptedRpcSender::new(w, w_key),
            call_handler,
        )
        .await
    }

    async fn connect_with_parts<R, W, H>(
        mut rx: R,
        tx: W,
        call_handler: H,
    ) -> RpcResult<RpcClient<W>>
    where
        R: RpcAsyncReceiver + Send + 'static,
        W: RpcAsyncSender + Send + 'static,
        H: RpcService + Clone + Send + Sync + 'static,
    {
        let state = Arc::new(ClientState::new(tx));
        let cloned_state = Arc::clone(&state);

        let task = tokio::spawn(async move {
            loop {
                match rx.receive().await {
                    Ok(message) => {
                        if let Err(e) =
                            Self::process_incoming(message, &call_handler, &cloned_state).await
                        {
                            log::error!("Receive task error: {e}");
                            break;
                        }
                    }
                    Err(e) => {
                        log::info!("Receive error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(RpcClient::new(state, task))
    }

    async fn process_incoming<W, H: RpcService>(
        message: Message,
        service: &H,
        state: &ClientState<W>,
    ) -> RpcResult<()>
    where
        W: RpcAsyncSender + 'static,
    {
        match message.kind {
            MessageType::Reply(_) | MessageType::Error(_) => {
                let mut pending = state.pending.lock().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Ok(message.kind));
                }
            }
            MessageType::Call(call) => {
                match service.call(&call).await {
                    Ok(reply) => {
                        let reply_msg = Message::reply(message.id, reply);
                        return state.sender.lock().await.send(&reply_msg).await;
                    }
                    Err(e) => {
                        let error_msg = Message::error(message.id, e);
                        return state.sender.lock().await.send(&error_msg).await;
                    }
                };
            }
            MessageType::Ping => {
                let pong = Message::pong(message.id);
                return state.sender.lock().await.send(&pong).await;
            }
            MessageType::Pong => {
                let mut pending = state.pending.lock().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Ok(message.kind));
                }
            }
            _ => {
                log::warn!("Received unexpected message type: {:?}", message.kind);
            }
        }
        Ok(())
    }

    /// Sends a message and waits for response.
    async fn send_message(
        &self,
        message: &Message,
        timeout_duration: Duration,
    ) -> RpcResult<MessageType> {
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
            Ok(Err(_)) => {
                // Channel was closed without response.
                let mut pending = self.state.pending.lock().await;
                pending.remove(&id);
                Err(RpcError::error(ErrKind::DroppedMessage))
            }
            Err(_) => {
                // Timeout occurred.
                let mut pending = self.state.pending.lock().await;
                pending.remove(&id);
                Err(RpcError::error(ErrKind::Timeout))
            }
        }
    }

    /// Sends a `ping`` message.
    pub async fn ping(&self, timeout: Duration) -> RpcResult<()> {
        let _ = self.send_message(&Message::ping(), timeout).await?;
        Ok(())
    }

    /// Sends a notification without response.
    pub async fn notify<P>(&self, method: u16, params: P) -> RpcResult<()>
    where
        P: Serialize,
    {
        let message = Message::notification_with(method, params)?;
        self.state.sender.lock().await.send(&message).await
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
            MessageType::Reply(reply) => {
                let result: R = Message::decode_as(&reply.data)?;
                Ok(result)
            }
            MessageType::Error(err) => Err(err),
            _ => Err(RpcError::error(ErrKind::UnexpectedMsg)),
        }
    }

    /// Consumes the instance and aborts its background task.
    /// Pending requests will be dropped.
    pub async fn shutdown(self) -> RpcResult<()> {
        self.task.abort();
        self.state.sender.lock().await.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Call, Reply};
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::net::{TcpStream, tcp};

    #[tokio::test]
    async fn test_client_call_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut io_stream, _) = listener.accept().await.unwrap();

            negotiation::accept_capability(&mut io_stream, RpcCapability::new(1, false))
                .await
                .expect("Server error: negotiation failed");

            let (r, w) = io_stream.owned_split();
            let mut rpc_reader = RpcReceiver::new(r);
            let mut rpc_writer = RpcSender::new(w);

            // Read the message (dynamically typed)
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

        let stream = TcpStream::connect(addr).await.unwrap();
        let client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>>::connect(stream, ())
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
            let (mut io_stream, _) = listener.accept().await.unwrap();

            let consensus =
                negotiation::accept_capability(&mut io_stream, RpcCapability::new(1, true))
                    .await
                    .expect("Server negotiation failed");

            assert!(consensus.encryption);

            let (r_key, w_key) = negotiation::accept_key_exchange(&mut io_stream)
                .await
                .expect("Server encryption setup failed");

            let (r, w) = io_stream.owned_split();
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

        let stream = TcpStream::connect(addr).await.unwrap();
        let client =
            RpcClient::<EncryptedRpcSender<tcp::OwnedWriteHalf>>::connect_encrypted(stream, ())
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
            let (mut io_stream, _) = listener.accept().await.unwrap();

            negotiation::accept_capability(&mut io_stream, RpcCapability::new(1, false))
                .await
                .expect("Server error: negotiation failed");

            let (r, w) = io_stream.owned_split();
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

        let stream = TcpStream::connect(addr).await.unwrap();
        let client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>>::connect(stream, ())
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
            let (mut io_stream, _) = listener.accept().await.unwrap();

            negotiation::accept_capability(&mut io_stream, RpcCapability::new(1, false))
                .await
                .expect("Server error: negotiation failed");

            let (r, _) = io_stream.owned_split();
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

        let stream = TcpStream::connect(addr).await.unwrap();
        let client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>>::connect(stream, ())
            .await
            .unwrap();

        client.notify(1, "some event").await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(*received_notification.lock().await);

        server_task.await.unwrap();
        client.shutdown().await.unwrap();
    }

    #[derive(Clone)]
    struct ClientHandler {
        counter: Arc<AtomicU32>,
    }

    impl RpcService for ClientHandler {
        async fn call(&self, call: &Call) -> RpcResult<Reply> {
            match call.method {
                1 => {
                    let current = self.counter.fetch_add(1, Ordering::SeqCst);
                    Ok(Reply::with(&(current + 1)).unwrap())
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
            let (mut io_stream, _) = listener.accept().await.unwrap();

            negotiation::accept_capability(&mut io_stream, RpcCapability::new(1, false))
                .await
                .expect("Server error: negotiation failed");

            let (r, w) = io_stream.owned_split();
            let mut rpc_reader = RpcReceiver::new(r);
            let mut rpc_writer = RpcSender::new(w);

            let server_call = Message::call_with(1, ()).unwrap();
            let _ = rpc_writer.send(&server_call).await;

            loop {
                match rpc_reader.receive().await {
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Reply(reply) => {
                                assert_eq!(Some(message.id), Some(server_call.id));
                                let result: u32 = Message::decode_as(&reply.data).unwrap();
                                assert_eq!(result, 1);
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

        let handler = ClientHandler {
            counter: Arc::new(AtomicU32::new(0)),
        };

        let stream = TcpStream::connect(addr).await.unwrap();
        let client = RpcClient::<RpcSender<tcp::OwnedWriteHalf>>::connect(stream, handler)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        server_task.await.unwrap();
        client.shutdown().await.unwrap()
    }
}
