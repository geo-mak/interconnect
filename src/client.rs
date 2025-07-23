use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::time::timeout;

use uuid::Uuid;

use serde::{Deserialize, Serialize};

use crate::error::ErrKind;
use crate::service::RpcService;
use crate::transport::AsyncIOStream;
use crate::{Message, MessageType, RpcError, RpcResult, RpcStream};

struct ClientState<S, H>
where
    S: AsyncIOStream,
    H: RpcService,
{
    service: H,
    stream: RpcStream<S>,
    pending: RwLock<HashMap<Uuid, Sender<RpcResult<MessageType>>>>,
}

/// RPC Client implementation.
pub struct RpcClient<S, H>
where
    S: AsyncIOStream,
    H: RpcService,
{
    state: Arc<ClientState<S, H>>,
    task: tokio::task::JoinHandle<RpcResult<()>>,
}

impl<S, H> RpcClient<S, H>
where
    S: AsyncIOStream + 'static,
    H: RpcService + 'static,
{
    /// Creates a new RPC client with the given RPC stream.
    pub fn new(io_stream: S, call_handler: H) -> Self {
        let state = Arc::new(ClientState {
            stream: RpcStream::new(io_stream),
            service: call_handler,
            pending: RwLock::new(HashMap::new()),
        });

        let cloned_state = Arc::clone(&state);

        // Processing incoming messages.
        let handle = tokio::spawn(async move {
            loop {
                match cloned_state.stream.read().await {
                    Ok(message) => {
                        Self::process_incoming_message(message, &cloned_state).await;
                    }
                    Err(e) => {
                        if matches!(e.kind, ErrKind::ConnectionClosed) {
                            log::info!("Connection closed");
                            return Err(e);
                        }
                        log::error!("Error receiving message: {e}");
                    }
                }
            }
        });

        Self {
            state,
            task: handle,
        }
    }

    /// Process incoming messages.
    async fn process_incoming_message(message: Message, state: &ClientState<S, H>) {
        match message.kind {
            MessageType::Reply(_) | MessageType::Error(_) => {
                let mut pending = state.pending.write().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Ok(message.kind));
                }
            }
            MessageType::Call(payload) => {
                let reply_data = match state
                    .service
                    .handle_call(payload.method, &payload.data)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        let error_msg = Message::error(message.id, e);
                        let _ = state.stream.write(&error_msg).await;
                        return;
                    }
                };
                let reply = Message::reply(message.id, reply_data);
                let _ = state.stream.write(&reply).await;
            }
            MessageType::Ping => {
                let pong = Message::pong(message.id);
                let _ = state.stream.write(&pong).await;
            }
            MessageType::Pong => {
                let mut pending = state.pending.write().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Ok(message.kind));
                }
            }
            _ => {
                log::warn!("Received unexpected message type: {:?}", message.kind);
            }
        }
    }

    /// Sends a `ping`` message.
    pub async fn ping(&self, timeout: Duration) -> RpcResult<()> {
        let _ = self.send_message(Message::ping(), timeout).await?;
        Ok(())
    }

    /// Sends a notification without response.
    pub async fn notify<P>(&self, method: u16, params: P) -> RpcResult<()>
    where
        P: Serialize,
    {
        let message = Message::notification_with(method, params)?;
        // Use transport directly.
        self.state.stream.write(&message).await
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

        let response = self.send_message(message, timeout).await?;

        match response {
            MessageType::Reply(reply) => {
                let result: R = Message::decode_as(&reply.data)?;
                Ok(result)
            }
            MessageType::Error(err) => Err(err),
            _ => Err(RpcError::error(ErrKind::UnexpectedMsg)),
        }
    }

    /// Sends a message and wait for response.
    async fn send_message(
        &self,
        message: Message,
        timeout_duration: Duration,
    ) -> RpcResult<MessageType> {
        let (sender, receiver) = oneshot::channel();

        let id = message.id;

        // Store pending request.
        {
            let mut pending = self.state.pending.write().await;
            pending.insert(id, sender);
        }

        // Send the message.
        if let Err(e) = self.state.stream.write(&message).await {
            let mut pending = self.state.pending.write().await;
            pending.remove(&id);
            return Err(e);
        }

        // Wait for response with timeout.
        match timeout(timeout_duration, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                // Channel was closed without response.
                let mut pending = self.state.pending.write().await;
                pending.remove(&id);
                Err(RpcError::error(ErrKind::ConnectionClosed))
            }
            Err(_) => {
                // Timeout occurred.
                let mut pending = self.state.pending.write().await;
                pending.remove(&id);
                Err(RpcError::error(ErrKind::Timeout))
            }
        }
    }

    /// Consumes the instance and aborts its background task.
    /// Pending requests will be dropped.
    pub async fn shutdown(self) -> RpcResult<()> {
        self.task.abort();
        self.state.stream.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_client_call_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let rpc_stream = RpcStream::new(stream);

            loop {
                match rpc_stream.read().await {
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Call(call) => {
                                assert_eq!(call.method, 1);
                                let params: String = Message::decode_as(&call.data).unwrap();
                                assert_eq!(params, "call".to_string());
                                // Send a response.
                                let response =
                                    Message::reply_with(message.id, "reply".to_string()).unwrap();
                                let _ = rpc_stream.write(&response).await;
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
            let (stream, _) = listener.accept().await.unwrap();
            let rpc_stream = RpcStream::new(stream);

            loop {
                match rpc_stream.read().await {
                    Ok(message) => {
                        match &message.kind {
                            MessageType::Call(call) => {
                                assert_eq!(call.method, 2);
                                let error = Message::error(
                                    message.id,
                                    RpcError::error(ErrKind::NotImplemented),
                                );
                                let _ = rpc_stream.write(&error).await;
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
            let (stream, _) = listener.accept().await.unwrap();
            let rpc_stream = RpcStream::new(stream);

            loop {
                match rpc_stream.read().await {
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

    struct ClientHandler {
        counter: Arc<AtomicU32>,
    }

    impl RpcService for ClientHandler {
        async fn handle_call(&self, method: u16, _data: &[u8]) -> RpcResult<Vec<u8>> {
            match method {
                1 => {
                    let current = self.counter.fetch_add(1, Ordering::SeqCst);
                    Ok(Message::encode_to_bytes(&(current + 1)).unwrap())
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
            let (stream, _) = listener.accept().await.unwrap();
            let rpc_stream = RpcStream::new(stream);

            let server_call = Message::call_with(1, ()).unwrap();
            let _ = rpc_stream.write(&server_call).await;

            loop {
                match rpc_stream.read().await {
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
