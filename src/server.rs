use std::fmt::Debug;
use std::marker::PhantomData;

use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

use crate::connection::OwnedSplitStream;
use crate::connection::RpcListener;
use crate::error::RpcResult;
use crate::message::{Message, MessageType};
use crate::service::RpcService;
use crate::transport::{AsyncRpcReceiver, AsyncRpcSender};

/// RPC Server implementation.
pub struct RpcServer<L, A, H>
where
    L: RpcListener<A>,
    A: Debug,
    H: RpcService,
{
    service: H,
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
            service,
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
                        tokio::spawn(Self::new_connection(io_stream, service.clone()));
                    }
                    Err(e) => {
                        log::error!("Failed to accept connection: {e}");
                    }
                }
            }
        });

        Ok(handle)
    }

    // TODO: Safe abort, if requested.
    async fn new_connection(io_stream: L::Stream, service: H) {
        let (io_reader, io_writer) = io_stream.owned_split();
        let mut rpc_rx = AsyncRpcReceiver::new(io_reader);
        let mut rpc_tx = AsyncRpcSender::new(io_writer);

        // TODO: Multiplexing strategy.
        loop {
            match rpc_rx.receive().await {
                Ok(message) => {
                    if let Err(e) =
                        Self::process_incoming_message(&message, &service, &mut rpc_tx).await
                    {
                        log::error!("Failed to send response to client: {e}");
                        break;
                    }
                }
                Err(e) => {
                    log::info!("Receive error: {e}");
                    break;
                }
            }
        }
    }

    async fn process_incoming_message<T>(
        message: &Message,
        service: &H,
        sender: &mut AsyncRpcSender<T>,
    ) -> RpcResult<()>
    where
        T: AsyncWriteExt + Send + Sync + Unpin,
    {
        match &message.kind {
            MessageType::Call(call) => match service.call(call).await {
                Ok(result) => sender.send(&Message::reply(message.id, result)).await,
                Err(err) => sender.send(&Message::error(message.id, err)).await,
            },
            MessageType::Notification(notify) => {
                // Notification, no reply.
                let _ = service.notify(notify).await;
                Ok(())
            }
            MessageType::Ping => sender.send(&Message::pong(message.id)).await,
            _ => {
                log::warn!("Received unexpected message type: {:?}", message.kind);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::message::{Call, Reply};

    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

    #[derive(Clone)]
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
        async fn call(&self, call: &Call) -> RpcResult<Reply> {
            let count = self.counter.fetch_add(1, Ordering::SeqCst);
            let response = format!(
                "Method called {} times. Data size: {}",
                count + 1,
                call.data.len()
            );
            Ok(Reply::with(&response)?)
        }
    }

    #[tokio::test]
    async fn test_tcp_rpc_server() {
        let service = RpcTestService::new();
        let mut server = RpcServer::<TcpListener, &str, RpcTestService>::new(service);

        let handle = server.start("127.0.0.1:8000").await.unwrap();

        let io_stream = TcpStream::connect("127.0.0.1:8000").await.unwrap();
        let (io_reader, io_writer) = io_stream.owned_split();

        let mut rpc_reader = AsyncRpcReceiver::new(io_reader);
        let mut rpc_writer = AsyncRpcSender::new(io_writer);

        let payload = b"hi there".to_vec();
        let call_msg = Message::call(1, payload.clone());
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
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

        let io_stream = UnixStream::connect(path).await.unwrap();
        let (io_reader, io_writer) = io_stream.owned_split();

        let mut rpc_reader = AsyncRpcReceiver::new(io_reader);
        let mut rpc_writer = AsyncRpcSender::new(io_writer);

        tokio::time::sleep(Duration::from_millis(10)).await;

        let payload = b"hi there".to_vec();
        let call_msg = Message::call(1, payload.clone());
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
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
