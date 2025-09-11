use std::cell::Cell;
use std::fmt::Debug;

use std::mem::ManuallyDrop;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::select;

use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::capability::{EncryptionState, negotiation};
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Message, MessageType};
use crate::service::RpcService;
use crate::stream::{
    EncryptedRpcReceiver, EncryptedRpcSender, RpcAsyncReceiver, RpcAsyncSender, RpcReceiver,
    RpcSender,
};
use crate::sync::{AtomicWaker, DynamicLatch, IList, INode};
use crate::transport::{TransportLayer, TransportListener};

thread_local! {
    // Must be non-zero.
    static RNG_STATE: Cell<u64> = const { Cell::new(0x12345678ABCDEF) };
}

struct TaskControlState {
    canceled: AtomicBool,
    waiter: AtomicWaker,
}

impl TaskControlState {
    const fn new() -> Self {
        Self {
            canceled: AtomicBool::new(false),
            waiter: AtomicWaker::new(),
        }
    }

    fn cancel(&self) {
        self.canceled.store(true, Ordering::Relaxed);
        self.waiter.wake();
    }

    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Relaxed)
    }
}

struct Tasks {
    shards: Box<[parking_lot::Mutex<IList<TaskControlState>>]>,
    observer: DynamicLatch,
}

unsafe impl Send for Tasks {}
unsafe impl Sync for Tasks {}

impl Tasks {
    fn new(shards: usize) -> Self {
        assert!(
            shards.is_power_of_two(),
            "Shards' count must be power of two"
        );
        let shards: Vec<_> = (0..shards)
            .map(|_| parking_lot::Mutex::new(IList::new()))
            .collect();
        Self {
            shards: shards.into_boxed_slice(),
            observer: DynamicLatch::new(),
        }
    }

    /// Selects a shard within the range of allocated shards randomly.
    ///
    /// The algorithm has distribution property to prevent clustering.
    fn select_shard(&self) -> usize {
        RNG_STATE.with(|s| {
            let mut x = s.get();
            // xorshift64s, variant (12, 25, 27).
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            s.set(x);
            // Len must be power of two.
            let mask = self.shards.len() - 1;
            (x.wrapping_mul(0x2545F4914F6CDD1D) as usize) & mask
        })
    }

    fn attach<'a, H>(
        &'a self,
        node: &'a mut TaskNode,
        state: &'a Arc<ServerState<H>>,
    ) -> AttachedTaskNode<'a, H> {
        let shard = self.select_shard();
        unsafe { self.shards[shard].lock().attach_first(&mut node.i_node) };
        AttachedTaskNode {
            t_node: node,
            state,
            shard,
        }
    }

    fn detach<H>(&self, node: &mut AttachedTaskNode<'_, H>) {
        unsafe {
            self.shards[node.shard]
                .lock()
                .detach(&mut node.t_node.i_node)
        };
    }
}

struct TaskNode {
    i_node: INode<TaskControlState>,
}

unsafe impl Send for TaskNode {}
unsafe impl Sync for TaskNode {}

impl TaskNode {
    #[inline(always)]
    fn new(state: TaskControlState) -> Self {
        Self {
            i_node: INode::new(state),
        }
    }
}

struct AttachedTaskNode<'a, H> {
    // Must be by ref.
    state: &'a Arc<ServerState<H>>,
    t_node: &'a mut TaskNode,
    shard: usize,
}

impl<'a, H> Drop for AttachedTaskNode<'a, H> {
    fn drop(&mut self) {
        self.state.tasks.detach(self);
    }
}

impl<'a, H> AttachedTaskNode<'a, H> {
    #[inline(always)]
    fn notify_canceled(&self) -> NotifyCanceledFuture<'_, H> {
        NotifyCanceledFuture { attached: self }
    }
}

struct NotifyCanceledFuture<'a, H> {
    attached: &'a AttachedTaskNode<'a, H>,
}

impl<'a, H> Future for NotifyCanceledFuture<'a, H> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.attached.t_node.i_node.is_canceled() {
            return Poll::Ready(());
        };
        self.attached.t_node.i_node.waiter.set(cx.waker());
        Poll::Pending
    }
}

struct ServerState<H> {
    tasks: Tasks,
    service: H,
    timeout: Duration,
}

impl<H> ServerState<H> {
    #[inline]
    fn new(shards: usize, service: H, timeout: Duration) -> ServerState<H> {
        ServerState {
            tasks: Tasks::new(shards),
            service,
            timeout,
        }
    }
}

/// RPC Server implementation.
pub struct RpcServer<H> {
    state: Arc<ServerState<H>>,
    listener: JoinHandle<()>,
}

impl<H> RpcServer<H>
where
    H: RpcService + Send + Sync + Clone + 'static,
{
    /// Initializes server state and starts accepting connections according to the given address and port.
    ///
    /// Shards' count is the count of concurrent lists used to track and manage its connections.
    ///
    /// Shards' count must be power of two.
    ///
    /// A higher count allocates more memory, but reduces contention and contributes to higher total system throughput.
    ///
    /// # Policy
    ///
    /// Encryption:
    /// If encryption is required, unencrypted sessions will be rejected during negotiation.
    ///
    /// If encryption is not required, encrypted sessions will still be served,
    /// it is just that unencrypted sessions will now be served as well.
    ///
    /// Timeout:
    /// timeout determines the allowed negotiation time before establishing session.
    /// There is no default, the provided value is used as it is.
    ///
    /// # Service
    ///
    /// Each new session gets its own clone of the service.
    ///
    /// # Shutdown
    ///
    /// System shutdown is performed with these steps in order:
    ///
    /// 1 - Stops accepting new connections with immediate effect.
    /// 2 - Signals termination to active sessions.
    /// 3 - Waits for all active sessions to finish properly.
    /// 4 - Calls shutdown on the service to inform it to terminates its state machines,
    ///     and waits for its completion.
    pub async fn serve<A, L>(
        addr: A,
        service: H,
        shards: usize,
        encryption_required: bool,
        conn_timeout: Duration,
    ) -> RpcResult<Self>
    where
        A: 'static,
        L: TransportListener<A> + Send + 'static,
        <L as TransportListener<A>>::Address: Debug + Send,
    {
        let listener = L::bind(addr).await?;

        let state = Arc::new(ServerState::new(shards, service, conn_timeout));

        let l_state = state.clone();
        let listener = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((transport, addr)) => {
                        let t_state = l_state.clone();
                        tokio::spawn(async move {
                            // TaskNode:
                            // |--- i_node: INode<TaskControlState>
                            //      |-- prev: ptr INode<TaskControlState>  | Pointers:
                            //      |-- next: ptr INode<TaskControlState>  | Accessed locked when attaching and detaching.
                            //      |-- data: TaskControlState
                            //            |-- canceled: AtomicBool         | Data:
                            //            |-- waiter: AtomicWaker          | Accessed directly by attached node and its future.
                            //                                             | Accessed locked but concurrently by shutdown.
                            // Safety:
                            // - The node and its control state are stored on the future and valid only as long
                            //   the future is still alive.
                            // - The address of the node is "assumed" to be stable,
                            //   because futures are constructed as "pinned" state machines.
                            // - Updating the node's next/prev and accessing its data can be concurrent.
                            // - The waker is atomic, set and wake can be concurrent.
                            let ctrl_state = TaskControlState::new();
                            let mut task_node = TaskNode::new(ctrl_state);

                            // Detached on drop.
                            // If it has stuck or it was late, acquiring a token will fail for sure.
                            let attached = t_state.tasks.attach(&mut task_node, &t_state);

                            // Token released on drop.
                            if let Some(_token) = t_state.tasks.observer.acquire() {
                                // Can panic.
                                let result = Self::connection::<L::Transport>(
                                    &attached,
                                    transport,
                                    encryption_required,
                                )
                                .await;

                                if let Err(e) = result {
                                    log::error!("Session with {addr:?} finished with error: {e}")
                                };

                                if attached.t_node.i_node.is_canceled() {
                                    log::info!("Session with {addr:?} canceled by shutdown");
                                    // Already detached by shutdown.
                                    let _ = ManuallyDrop::new(attached);
                                }
                            }
                        });
                    }
                    Err(e) => log::error!("Failed to accept connection: {e}"),
                }
            }
        });

        Ok(Self { state, listener })
    }

    /// Tries to negotiate a new session and starts one over the transport layer upon success.
    async fn connection<T>(
        node: &AttachedTaskNode<'_, H>,
        mut transport: T,
        encryption_required: bool,
    ) -> RpcResult<()>
    where
        T: TransportLayer,
    {
        let encrypted = timeout(
            node.state.timeout,
            Self::negotiation(&mut transport, encryption_required),
        )
        .await??;

        let (r, w) = transport.into_split();
        match encrypted {
            None => {
                let mut s = RpcSender::new(w);
                let mut r = RpcReceiver::new(r);
                Self::session(node, &mut s, &mut r).await
            }
            Some((r_key, w_key)) => {
                let mut s = EncryptedRpcSender::new(w, w_key);
                let mut r = EncryptedRpcReceiver::new(r, r_key);
                Self::session(node, &mut s, &mut r).await
            }
        }
    }

    #[inline]
    async fn negotiation<T>(
        mut transport: &mut T,
        encryption_required: bool,
    ) -> RpcResult<Option<(EncryptionState, EncryptionState)>>
    where
        T: TransportLayer,
    {
        let proposed = negotiation::read_frame(&mut transport).await?;

        if proposed.version != 1 {
            negotiation::reject(&mut transport).await?;
            return Err(RpcError::error(ErrKind::CapabilityMismatch));
        };

        if proposed.encryption {
            negotiation::confirm(&mut transport).await?;
            let (r_key, w_key) = negotiation::accept_key_exchange(&mut transport).await?;
            Ok(Some((r_key, w_key)))
        } else {
            if encryption_required {
                negotiation::reject(&mut transport).await?;
                return Err(RpcError::error(ErrKind::CapabilityMismatch));
            }
            negotiation::confirm(&mut transport).await?;
            Ok(None)
        }
    }

    async fn session<S, R>(
        node: &AttachedTaskNode<'_, H>,
        sender: &mut S,
        receiver: &mut R,
    ) -> RpcResult<()>
    where
        S: RpcAsyncSender,
        R: RpcAsyncReceiver,
    {
        let service = node.state.service.clone();
        loop {
            select! {
                biased;
                // Cancellation is intended, so there is no error.
                 _ = node.notify_canceled() => return Ok(()),
            result = receiver.receive() => {
                    match result {
                        Ok(message) => Self::process_message(&service, sender, &message).await?,
                        Err(e) => return Err(e),
                    };
                }
            }
        }
    }

    async fn process_message<S>(service: &H, sender: &mut S, message: &Message) -> RpcResult<()>
    where
        S: RpcAsyncSender,
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

    /// Returns the current count of active sessions.
    #[inline(always)]
    pub fn sessions(&self) -> usize {
        self.state.tasks.observer.count()
    }

    /// Shutdowns the server and the service in planned mode.
    ///
    /// This call doesn't have immediate effect and may take longer time,
    /// because it allows active sessions to complete processing the current received message.
    pub async fn shutdown(&mut self) -> RpcResult<()> {
        self.state.tasks.observer.open();
        self.listener.abort();

        for shard in &self.state.tasks.shards {
            shard.lock().drain(|t| t.cancel());
        }

        self.state.tasks.observer.wait().await;

        self.state.service.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

    use crate::capability::{self, RpcCapability};
    use crate::error::{ErrKind, RpcError};
    use crate::message::{Call, Reply};

    #[derive(Clone)]
    struct RpcTestService {}

    impl RpcService for RpcTestService {
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

    async fn make_tcp_rpc_channel(server: &str) -> (impl RpcAsyncSender, impl RpcAsyncReceiver) {
        let mut transport = TcpStream::connect(server).await.unwrap();

        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = transport.into_split();
        (RpcSender::new(writer), RpcReceiver::new(reader))
    }

    async fn make_encrypted_tcp_rpc_channel(
        server: &str,
    ) -> (impl RpcAsyncSender, impl RpcAsyncReceiver) {
        let mut transport = TcpStream::connect(server).await.unwrap();

        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, true))
            .await
            .expect("client negotiation failed");

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut transport)
            .await
            .expect("client encryption setup failed");

        let (r, w) = transport.into_split();

        (
            EncryptedRpcSender::new(w, w_key),
            EncryptedRpcReceiver::new(r, r_key),
        )
    }

    #[tokio::test]
    async fn test_tcp_rpc_server_core() {
        let srv_addr = "127.0.0.1:8000";
        let service = RpcTestService {};
        let mut server = RpcServer::serve::<&str, TcpListener>(
            srv_addr,
            service,
            2,
            false,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let (mut rpc_tx_1, mut rpc_rx_1) = make_tcp_rpc_channel(srv_addr).await;
        let (mut rpc_tx_2, mut rpc_rx_2) = make_tcp_rpc_channel(srv_addr).await;
        let (mut rpc_tx_3, mut rpc_rx_3) = make_tcp_rpc_channel(srv_addr).await;

        let rpc_call_1 = Message::call_with(1, "C1").unwrap();
        let rpc_call_2 = Message::call_with(1, "C2").unwrap();
        let rpc_call_3 = Message::call_with(1, "C3").unwrap();

        rpc_tx_1.send(&rpc_call_1).await.unwrap();
        rpc_tx_2.send(&rpc_call_2).await.unwrap();
        rpc_tx_3.send(&rpc_call_3).await.unwrap();

        let t1 = tokio::spawn(async move {
            let reply_msg = rpc_rx_1.receive().await.unwrap();
            match reply_msg.kind {
                MessageType::Reply(reply) => {
                    let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                    assert!(&response == "Reply to C1");
                }
                _ => panic!("Expected reply"),
            }
        });

        let t2 = tokio::spawn(async move {
            let reply_msg = rpc_rx_2.receive().await.unwrap();
            match reply_msg.kind {
                MessageType::Reply(reply) => {
                    let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                    assert!(&response == "Reply to C2");
                }
                _ => panic!("Expected reply"),
            }
        });

        let t3 = tokio::spawn(async move {
            let reply_msg = rpc_rx_3.receive().await.unwrap();
            match reply_msg.kind {
                MessageType::Reply(reply) => {
                    let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                    assert!(&response == "Reply to C3");
                }
                _ => panic!("Expected reply"),
            }
        });

        tokio::try_join!(t1, t2, t3).expect("No panic expected");

        server.shutdown().await.unwrap();

        assert!(server.listener.is_finished());
        assert!(server.sessions() == 0);
        assert!(Arc::weak_count(&server.state) == 0);
        assert!(Arc::strong_count(&server.state) == 1);
    }

    #[tokio::test]
    async fn test_tcp_rpc_server_encryption_policy() {
        let srv_addr = "127.0.0.1:8001";
        let service = RpcTestService {};

        // Server with encryption-only policy.
        let mut server = RpcServer::serve::<&str, TcpListener>(
            srv_addr,
            service,
            2,
            true,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        // Unencrypted session.
        {
            let mut transport = TcpStream::connect(srv_addr).await.unwrap();
            let response =
                capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
                    .await;
            assert!(response == Err(RpcError::error(ErrKind::CapabilityMismatch)));
        };

        let (mut rpc_tx, mut rpc_rx) = make_encrypted_tcp_rpc_channel(srv_addr).await;

        let call_msg = Message::call_with(1, "C1").unwrap();
        rpc_tx.send(&call_msg).await.unwrap();

        let reply_msg = rpc_rx.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(&response == "Reply to C1");
            }
            _ => panic!("Expected reply"),
        }

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_unix_rpc_server() {
        let path = "unix_server_test.sock";

        let service = RpcTestService {};
        let mut server =
            RpcServer::serve::<&str, UnixListener>(path, service, 2, false, Duration::from_secs(1))
                .await
                .unwrap();

        let mut transport = UnixStream::connect(path).await.unwrap();

        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = transport.into_split();
        let mut rpc_rx = RpcReceiver::new(reader);
        let mut rpc_tx = RpcSender::new(writer);

        tokio::time::sleep(Duration::from_millis(10)).await;

        let call_msg = Message::call_with(1, "C1").unwrap();
        rpc_tx.send(&call_msg).await.unwrap();

        let reply_msg = rpc_rx.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(&response == "Reply to C1");
            }
            _ => panic!("Expected reply"),
        }

        server.shutdown().await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
