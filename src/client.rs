use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use tokio::time::timeout;

use uuid::Uuid;

use serde::{Deserialize, Serialize};

use crate::connection::OwnedSplitStream;
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Message, MessageType};
use crate::service::RpcService;
use crate::transport::{AsyncRpcReceiver, AsyncRpcSender};

struct ClientState<S, H>
where
    S: OwnedSplitStream,
    H: RpcService,
{
    service: H,
    stream: Mutex<AsyncRpcSender<S::OwnedWriteHalf>>,
    pending: Mutex<HashMap<Uuid, Sender<RpcResult<MessageType>>>>,
}

/// RPC Client implementation.
pub struct RpcClient<S, H>
where
    S: OwnedSplitStream,
    H: RpcService,
{
    state: Arc<ClientState<S, H>>,
    task: tokio::task::JoinHandle<RpcResult<()>>,
}

impl<S, H> RpcClient<S, H>
where
    S: OwnedSplitStream + 'static,
    H: RpcService + 'static,
{
    /// Creates a new RPC client with the given I/O stream.
    pub fn new(io_stream: S, call_handler: H) -> Self {
        let (io_reader, io_writer) = io_stream.owned_split();

        let mut rpc_reader = AsyncRpcReceiver::new(io_reader);
        let rpc_writer = AsyncRpcSender::new(io_writer);

        let state = Arc::new(ClientState {
            service: call_handler,
            stream: Mutex::new(rpc_writer),
            pending: Mutex::new(HashMap::new()),
        });

        let cloned_state = Arc::clone(&state);

        // Processing incoming messages.
        let task = tokio::spawn(async move {
            loop {
                // Stream is only being read here.
                match rpc_reader.receive().await {
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

        Self { state, task }
    }

    async fn process_incoming_message(message: Message, state: &ClientState<S, H>) {
        match message.kind {
            MessageType::Reply(_) | MessageType::Error(_) => {
                let mut pending = state.pending.lock().await;
                if let Some(sender) = pending.remove(&message.id) {
                    let _ = sender.send(Ok(message.kind));
                }
            }
            MessageType::Call(call) => {
                let reply = match state.service.call(&call).await {
                    Ok(result) => result,
                    Err(e) => {
                        let error_msg = Message::error(message.id, e);
                        let _ = state.stream.lock().await.send(&error_msg).await;
                        return;
                    }
                };
                let reply_msg = Message::reply(message.id, reply);
                let _ = state.stream.lock().await.send(&reply_msg).await;
            }
            MessageType::Ping => {
                let pong = Message::pong(message.id);
                let _ = state.stream.lock().await.send(&pong).await;
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
        self.state.stream.lock().await.send(&message).await
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

    /// Sends a message and waits for response.
    async fn send_message(
        &self,
        message: Message,
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
        if let Err(e) = self.state.stream.lock().await.send(&message).await {
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
                Err(RpcError::error(ErrKind::ConnectionClosed))
            }
            Err(_) => {
                // Timeout occurred.
                let mut pending = self.state.pending.lock().await;
                pending.remove(&id);
                Err(RpcError::error(ErrKind::Timeout))
            }
        }
    }

    /// Consumes the instance and aborts its background task.
    /// Pending requests will be dropped.
    pub async fn shutdown(self) -> RpcResult<()> {
        self.task.abort();
        self.state.stream.lock().await.close().await
    }
}

#[cfg(test)]
mod tests {
    use crate::message::{Call, Reply};

    use super::*;
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
