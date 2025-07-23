use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::connection::RpcListener;
use crate::error::{ErrKind, RpcResult};
use crate::message::{Message, MessageType};
use crate::service::RpcService;
use crate::transport::RpcStream;

/// RPC Server implementation.
pub struct RpcServer<L, A, H>
where
    L: RpcListener<A>,
    A: Debug,
    H: RpcService,
{
    service: Arc<H>,
    _l: PhantomData<L>,
    _a: PhantomData<A>,
}

impl<L, A, H> RpcServer<L, A, H>
where
    L: RpcListener<A> + Send + 'static,
    A: Debug + 'static,
    H: RpcService + 'static,
{
    pub fn new(service: H) -> Self {
        Self {
            service: Arc::new(service),
            _l: PhantomData,
            _a: PhantomData,
        }
    }

    pub async fn start(&mut self, addr: A) -> RpcResult<JoinHandle<()>> {
        let listener = L::bind(addr).await?;
        let service = self.service.clone();
        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((io_stream, _)) => {
                        // TODO: Needs handle.
                        Self::spawn_connection(io_stream, service.clone());
                    }
                    Err(e) => {
                        log::error!("Failed to accept TCP connection: {e}");
                    }
                }
            }
        });
        Ok(handle)
    }

    // TODO: Safe abort, if requested.
    fn spawn_connection(io_stream: L::Stream, service: Arc<H>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let rpc_stream = RpcStream::new(io_stream);
            loop {
                match rpc_stream.read().await {
                    Ok(message) => {
                        let result = Self::process_incoming_message(message, &service).await;
                        if let Some(reply_msg) = result {
                            if let Err(e) = rpc_stream.write(&reply_msg).await {
                                log::error!("Failed to send response to client: {e}");
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        if matches!(e.kind, ErrKind::ConnectionClosed) {
                            log::info!("Client disconnected");
                        } else {
                            log::error!("Error handling connection : {e}");
                        }
                        break;
                    }
                }
            }
            log::info!("Connection closed");
        })
    }

    async fn process_incoming_message(message: Message, service: &H) -> Option<Message> {
        match message.kind {
            MessageType::Call(call) => match service.handle_call(call.method, &call.data).await {
                Ok(result) => Some(Message::reply(message.id, result)),
                Err(err) => Some(Message::error(message.id, err)),
            },
            MessageType::Notification(notify) => {
                // Notification, no reply.
                let _ = service
                    .handle_notification(notify.method, &notify.data)
                    .await;
                None
            }
            MessageType::Ping => Some(Message::pong(message.id)),
            _ => {
                log::warn!("Received unexpected message type: {:?}", message.kind);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

    struct RpcTestService {
        counter: Arc<AtomicU32>,
    }

    impl RpcTestService {
        fn new() -> Self {
            Self {
                counter: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    impl RpcService for RpcTestService {
        async fn handle_call(&self, _method: u16, data: &[u8]) -> RpcResult<Vec<u8>> {
            let count = self.counter.fetch_add(1, Ordering::SeqCst);
            let response = format!(
                "Method called {} times. Data size: {}",
                count + 1,
                data.len()
            );
            Ok(Message::encode_to_bytes(&response)?)
        }
    }

    #[tokio::test]
    async fn test_tcp_rpc_server() {
        let service = RpcTestService::new();
        let mut server = RpcServer::<TcpListener, &str, RpcTestService>::new(service);

        let handle = server.start("127.0.0.1:8000").await.unwrap();

        let stream = TcpStream::connect("127.0.0.1:8000").await.unwrap();
        let client = RpcStream::new(stream);

        let payload = b"hi there".to_vec();
        let call_msg = Message::call(1, payload.clone());
        client.write(&call_msg).await.unwrap();

        let reply_msg = client.read().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(response.contains("Method called"));
                assert!(response.contains(&payload.len().to_string()));
            }
            _ => panic!("Expected reply"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn test_unix_rpc_server() {
        let path = "unix_server_test.sock";

        let service = RpcTestService::new();
        let mut server = RpcServer::<UnixListener, &str, RpcTestService>::new(service);

        let handle = server.start(path).await.unwrap();

        let stream = UnixStream::connect(path).await.unwrap();
        let client = RpcStream::new(stream);

        tokio::time::sleep(Duration::from_millis(10)).await;

        let payload = b"hi there".to_vec();
        let call_msg = Message::call(1, payload.clone());
        client.write(&call_msg).await.unwrap();

        let reply_msg = client.read().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(response.contains("Method called"));
                assert!(response.contains(&payload.len().to_string()));
            }
            _ => panic!("Expected reply"),
        }

        handle.abort();

        std::fs::remove_file(path).unwrap();
    }
}
