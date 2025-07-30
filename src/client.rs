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
use crate::service::RpcService;
use crate::transport::{AsyncRpcReceiver, AsyncRpcSender, OwnedSplitStream};

struct ClientState<S>
where
    S: OwnedSplitStream,
{
    sender: Mutex<AsyncRpcSender<S::OwnedWriteHalf>>,
    pending: Mutex<HashMap<Uuid, Sender<RpcResult<MessageType>>>>,
}

/// RPC Client implementation.
pub struct RpcClient<S>
where
    S: OwnedSplitStream,
{
    state: Arc<ClientState<S>>,
    task: tokio::task::JoinHandle<()>,
}

impl<S> RpcClient<S>
where
    S: OwnedSplitStream + 'static,
{
    /// Creates a new RPC client with the given I/O stream.
    pub fn new<H>(io_stream: S, call_handler: H) -> Self
    where
        H: RpcService,
    {
        let (io_reader, io_writer) = io_stream.owned_split();

        let state = Arc::new(ClientState {
            sender: Mutex::new(AsyncRpcSender::new(io_writer)),
            pending: Mutex::new(HashMap::new()),
        });

        let service = call_handler.clone();
        let cloned_state = Arc::clone(&state);
        let mut rpc_rx = AsyncRpcReceiver::new(io_reader);

        // TODO: Multiplexing strategy.
        let task = tokio::spawn(async move {
            loop {
                // Stream is only being read here.
                match rpc_rx.receive().await {
                    Ok(message) => {
                        if let Err(e) =
                            Self::process_incoming_message(message, &service, &cloned_state).await
                        {
                            log::error!("Failed to send response: {e}");
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

        Self { state, task }
    }

    async fn process_incoming_message<H: RpcService>(
        message: Message,
        service: &H,
        state: &ClientState<S>,
    ) -> RpcResult<()> {
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
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_client_call_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (io_stream, _) = listener.accept().await.unwrap();
            let (reader, writer) = io_stream.owned_split();

            let mut rpc_reader = AsyncRpcReceiver::new(reader);
            let mut rpc_writer = AsyncRpcSender::new(writer);

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
        let client = RpcClient::new(stream, ());

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
            let (io_stream, _) = listener.accept().await.unwrap();
            let (reader, writer) = io_stream.owned_split();

            let mut rpc_reader = AsyncRpcReceiver::new(reader);
            let mut rpc_writer = AsyncRpcSender::new(writer);

            loop {
                match rpc_reader.receive().await {
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Call(call) => {
                                assert_eq!(call.method, 2);
                                let error = Message::error(
                                    message.id,
                                    RpcError::error(ErrKind::NotImplemented),
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
        let client = RpcClient::new(stream, ());

        let result: Result<(), RpcError> = client.call(2, "call").await;

        match result {
            Ok(_) => panic!("Expected error result"),
            Err(e) => {
                assert!(e.kind == ErrKind::NotImplemented);
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
            let (io_stream, _) = listener.accept().await.unwrap();
            let (reader, _) = io_stream.owned_split();
            let mut rpc_reader = AsyncRpcReceiver::new(reader);
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
        let client = RpcClient::new(stream, ());

        client.notify(1, "some event").await.unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

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
                _ => Err(RpcError::error(ErrKind::NotImplemented)),
            }
        }
    }

    #[tokio::test]
    async fn test_client_bidirectional_call() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (io_stream, _) = listener.accept().await.unwrap();
            let (reader, writer) = io_stream.owned_split();

            let mut rpc_reader = AsyncRpcReceiver::new(reader);
            let mut rpc_writer = AsyncRpcSender::new(writer);

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
        let client = RpcClient::new(stream, handler);

        tokio::time::sleep(Duration::from_millis(10)).await;

        server_task.await.unwrap();
        client.shutdown().await.unwrap()
    }
}
