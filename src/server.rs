use std::cell::{Cell, UnsafeCell};
use std::fmt::Debug;
use std::marker::{PhantomData, PhantomPinned};
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pin_project_lite::pin_project;

use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::capability::{EncryptionState, negotiation};
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::message::{Call, Message, MessageID, MessageType, Reply};
use crate::report::Reporter;
use crate::service::{CallContext, RpcService};
use crate::stream::{
    AsyncRpcReceiver, AsyncRpcSender, EncryptedRpcReceiver, EncryptedRpcSender, RpcReceiver,
    RpcSender,
};
use crate::sync::{DynamicLatch, IList, INode, NOOP_WAKER};
use crate::transport::{TransportLayer, TransportListener};

thread_local! {
    // Must be non-zero.
    static RNG_STATE: Cell<u64> = const { Cell::new(0x12345678ABCDEF) };
}

struct Tasks {
    shards: Box<[parking_lot::Mutex<IList<TaskControlState>>]>,
    mask: usize,
    observer: DynamicLatch,
}

unsafe impl Send for Tasks {}
unsafe impl Sync for Tasks {}

impl Tasks {
    fn new(n_shards: usize) -> Self {
        assert!(
            n_shards.is_power_of_two(),
            "Shards' count must be power of two"
        );

        let shards: Vec<_> = (0..n_shards)
            .map(|_| parking_lot::Mutex::new(IList::new()))
            .collect();

        Self {
            shards: shards.into_boxed_slice(),
            mask: n_shards - 1,
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
            (x.wrapping_mul(0x2545F4914F6CDD1D) as usize) & self.mask
        })
    }

    fn attach<'a, H, E>(
        &'a self,
        task: &'a mut Task,
        state: &'a Arc<ServerState<H, E>>,
    ) -> Option<AttachedTask<'a, H, E>> {
        let shard = self.select_shard();
        let mut shard_lock = self.shards[shard].lock();
        if self.observer.acquire_manual() {
            unsafe {
                shard_lock.attach_first(&mut task.node);
                drop(shard_lock);
            };
            return Some(AttachedTask {
                task,
                srv_state: state,
                shard,
            });
        }
        None
    }

    fn detach<H, E>(&self, node: &mut AttachedTask<'_, H, E>) {
        unsafe { self.shards[node.shard].lock().detach(&mut node.task.node) };
    }
}

struct TaskControlState {
    state: AtomicU8,
    waker: UnsafeCell<Waker>,
    _pin: PhantomPinned,
}

const WAIT: u8 = 0b00;
const SET: u8 = 0b01;
const CANCEL: u8 = 0b10;
const SET_CANCEL: u8 = SET | CANCEL;

unsafe impl Send for TaskControlState {}
unsafe impl Sync for TaskControlState {}

impl TaskControlState {
    #[inline]
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(WAIT),
            waker: UnsafeCell::new(NOOP_WAKER),
            _pin: PhantomPinned,
        }
    }

    #[inline]
    fn cancel(&self) {
        match self.state.fetch_or(CANCEL, AcqRel) {
            WAIT => unsafe {
                (*self.waker.get()).wake_by_ref();
            },
            other => {
                debug_assert!(other == SET || other == CANCEL || other == SET_CANCEL);
            }
        }
    }

    #[inline(always)]
    fn is_canceled(&self) -> bool {
        self.state.load(Acquire) > SET
    }
}

struct Canceled;

pin_project! {
    struct CancelableTask<'a, F> {
        control: &'a TaskControlState,
        #[pin]
        future: F,
    }
}

impl<'a, F> Future for CancelableTask<'a, F>
where
    F: Future,
{
    type Output = Result<F::Output, Canceled>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.control.is_canceled() {
            return Poll::Ready(Err(Canceled));
        }

        if let Poll::Ready(x) = self.as_mut().project().future.poll(cx) {
            return Poll::Ready(Ok(x));
        }

        let state = &self.control.state;

        let observed = match state.compare_exchange(WAIT, SET, AcqRel, Acquire) {
            Ok(prev) => prev,
            Err(current) => current,
        };

        match observed {
            WAIT => unsafe {
                let waker_ptr = self.control.waker.get();

                if !(*waker_ptr).will_wake(cx.waker()) {
                    *waker_ptr = cx.waker().clone()
                }

                match state.compare_exchange(SET, WAIT, AcqRel, Acquire) {
                    Ok(_) => Poll::Pending,
                    Err(current) => {
                        debug_assert!(current == SET_CANCEL);
                        Poll::Ready(Err(Canceled))
                    }
                }
            },
            CANCEL | SET_CANCEL => Poll::Ready(Err(Canceled)),
            _ => unreachable!("Task is being polled concurrently"),
        }
    }
}

struct Task {
    node: INode<TaskControlState>,
}

unsafe impl Send for Task {}
unsafe impl Sync for Task {}

impl Task {
    #[inline(always)]
    const fn new() -> Self {
        Self {
            node: INode::new(TaskControlState::new()),
        }
    }
}

struct AttachedTask<'a, H, E> {
    // Must be by ref.
    srv_state: &'a Arc<ServerState<H, E>>,
    task: &'a mut Task,
    shard: usize,
}

impl<'a, H, E> Drop for AttachedTask<'a, H, E> {
    fn drop(&mut self) {
        self.srv_state.tasks.detach(self);
        self.srv_state.tasks.observer.release();
    }
}

impl<'a, H, E> AttachedTask<'a, H, E> {
    #[inline(always)]
    fn wait_cancelable<F>(&self, future: F) -> CancelableTask<'_, F> {
        CancelableTask {
            control: &self.task.node,
            future,
        }
    }

    #[inline(always)]
    fn release_undetached(self) {
        self.srv_state.tasks.observer.release();
        mem::forget(self);
    }
}

struct ServerState<H, E> {
    tasks: Tasks,
    service: H,
    report: E,
    timeout: Duration,
}

impl<H, E> ServerState<H, E> {
    #[inline]
    fn new(service: H, timeout: Duration, reporter: E, shards: usize) -> ServerState<H, E> {
        ServerState {
            tasks: Tasks::new(shards),
            service,
            report: reporter,
            timeout,
        }
    }
}

struct ServerContext<'a, S> {
    id: &'a MessageID,
    sender: &'a mut S,
}

impl<'a, S> ServerContext<'a, S>
where
    S: AsyncRpcSender + Send,
{
    #[inline(always)]
    const fn new(id: &'a MessageID, sender: &'a mut S) -> Self {
        Self { id, sender }
    }
}

impl<'a, S> CallContext for ServerContext<'a, S>
where
    S: AsyncRpcSender + Send,
{
    type ID = MessageID;

    #[inline(always)]
    fn id(&self) -> &Self::ID {
        self.id
    }

    #[inline]
    async fn send_reply(&mut self, reply: Reply) -> RpcResult<()> {
        self.sender.send(&Message::reply(*self.id, reply)).await
    }

    #[inline]
    async fn send_error(&mut self, err: RpcError) -> RpcResult<()> {
        self.sender.send(&Message::error(*self.id, err)).await
    }

    #[inline]
    async fn call(&mut self, call: Call) -> RpcResult<()> {
        self.sender
            .send(&Message::new(*self.id, MessageType::Call(call)))
            .await
    }

    #[inline]
    async fn call_nullary(&mut self, method: u16) -> RpcResult<()> {
        self.sender
            .send(&Message::new(*self.id, MessageType::NullaryCall(method)))
            .await
    }
}

/// RPC Server implementation.
pub struct RpcServer<A, H, E> {
    state: Arc<ServerState<H, E>>,
    listener: JoinHandle<()>,
    _a: PhantomData<A>,
}

impl<A, H, E> RpcServer<A, H, E>
where
    H: RpcService + Send + Sync + Clone + 'static,
    E: Reporter + Send + Sync + 'static,
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
    /// # Reporting
    ///
    /// Reporting requires a reporter instance, that must be passed explicitly as parameter.
    ///
    /// Reporter is the component responsible for the "logging" of server events.
    ///
    /// If reporting is not needed, the no-op implementation `()` can be passed as value.
    ///
    /// The server reports `errors` and `alerts` of unexpected messages only.
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
    pub async fn serve<L>(
        addr: A,
        service: H,
        encrypted_only: bool,
        timeout: Duration,
        reporter: E,
        shards: usize,
    ) -> RpcResult<Self>
    where
        A: 'static,
        L: TransportListener<A> + Send + 'static,
        <L as TransportListener<A>>::Address: Debug + Send,
    {
        let listener = L::bind(addr).await?;

        let state = Arc::new(ServerState::new(service, timeout, reporter, shards));

        let state_l = state.clone();
        let listener = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((transport, addr)) => {
                        let state_t = state_l.clone();
                        tokio::spawn(async move {
                            // Task:
                            // |--- i_node: INode<TaskControlState>
                            //      |-- prev: ptr INode<TaskControlState>  | Pointers:
                            //      |-- next: ptr INode<TaskControlState>  | Accessed locked when attaching and detaching.
                            //      |-- data: TaskControlState             | Data:
                            //                                             | Accessed directly by attached task and its future.
                            //                                             | Accessed locked but concurrently by shutdown.
                            //
                            // Safety:
                            // - The task and its control state are stored on the future and valid only as long
                            //   the future is still alive.
                            // - The address of the task is "assumed" to be stable,
                            //   because futures are constructed as "pinned" state machines.
                            // - Updating the task's node and accessing its data can be concurrent.
                            // - The state is atomic, updating and canceling can be concurrent.
                            let mut pinned_task = Task::new();

                            // Detached on drop with release effect.
                            if let Some(attached) = state_t.tasks.attach(&mut pinned_task, &state_t)
                            {
                                // Can panic.
                                let result = Self::connection::<L::Transport>(
                                    &attached,
                                    transport,
                                    encrypted_only,
                                )
                                .await;

                                if let Err(err) = result
                                    && err.kind != ErrKind::Disconnected
                                {
                                    state_t.report.error(
                                        "Session finished with error",
                                        &format_args!("{err}. Peer: {addr:?}"),
                                    )
                                };

                                // Detaching again is safe, but we try to avoid the "thundering herd" problem.
                                // This allows shutdown to access locks smoothly without contention.
                                if attached.task.node.is_canceled() {
                                    attached.release_undetached();
                                    state_t.report.info(
                                        "Session canceled by shutdown",
                                        &format_args!("Peer: {addr:?}"),
                                    );
                                }
                            }
                        });
                    }
                    Err(err) => state_l.report.error("Failed to accept connection", &err),
                }
            }
        });

        Ok(Self {
            state,
            listener,
            _a: PhantomData,
        })
    }

    /// Tries to negotiate a new session and starts one over the transport layer upon success.
    async fn connection<T>(
        task: &AttachedTask<'_, H, E>,
        mut transport: T,
        encryption_required: bool,
    ) -> RpcResult<()>
    where
        T: TransportLayer,
    {
        // TODO: Is it worth cancellation logic?
        let encrypted = timeout(
            task.srv_state.timeout,
            Self::negotiation(&mut transport, encryption_required),
        )
        .await??;

        let (r, w) = transport.into_split();
        match encrypted {
            None => {
                let mut s = RpcSender::new(w);
                let mut r = RpcReceiver::new(r);
                Self::session(task, &mut s, &mut r).await
            }
            Some((r_key, w_key)) => {
                let mut s = EncryptedRpcSender::new(w, w_key);
                let mut r = EncryptedRpcReceiver::new(r, r_key);
                Self::session(task, &mut s, &mut r).await
            }
        }
    }

    #[inline]
    async fn negotiation<T>(
        transport: &mut T,
        encryption_required: bool,
    ) -> RpcResult<Option<(EncryptionState, EncryptionState)>>
    where
        T: TransportLayer,
    {
        let proposed = negotiation::read_frame(transport).await?;

        if proposed.version != 1 {
            negotiation::reject(transport).await?;
            return Err(RpcError::error(ErrKind::CapabilityMismatch));
        };

        if proposed.encryption {
            negotiation::confirm(transport).await?;
            let (r_key, w_key) = negotiation::accept_key_exchange(transport).await?;
            Ok(Some((r_key, w_key)))
        } else {
            if encryption_required {
                negotiation::reject(transport).await?;
                return Err(RpcError::error(ErrKind::CapabilityMismatch));
            }
            negotiation::confirm(transport).await?;
            Ok(None)
        }
    }

    async fn session<S, R>(
        task: &AttachedTask<'_, H, E>,
        sender: &mut S,
        receiver: &mut R,
    ) -> RpcResult<()>
    where
        S: AsyncRpcSender + Send,
        R: AsyncRpcReceiver,
    {
        let service = task.srv_state.service.clone();
        loop {
            match task.wait_cancelable(receiver.receive()).await {
                Ok(result) => {
                    let message = result?;
                    match &message.kind {
                        MessageType::Call(call) => {
                            let mut context = ServerContext::new(&message.id, sender);
                            service.call(call, &mut context).await?
                        }
                        MessageType::NullaryCall(method) => {
                            let mut context = ServerContext::new(&message.id, sender);
                            service.call_nullary(*method, &mut context).await?
                        }
                        MessageType::Ping => sender.send(&Message::pong(message.id)).await?,
                        _ => {
                            task.srv_state
                                .report
                                .alert("Received unexpected message type: ", &message.kind);
                        }
                    }
                }
                Err(Canceled) => return Ok(()),
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
    use crate::report::STDIOReporter;

    #[derive(Clone)]
    struct RpcTestService {}

    impl RpcService for RpcTestService {
        async fn call<C>(&self, call: &Call, context: &mut C) -> RpcResult<()>
        where
            C: CallContext + Send,
        {
            match call.method {
                1 => {
                    let src = call.decode_as::<String>().unwrap();
                    context
                        .send_reply(Reply::with(&format!("Reply to {src}")).unwrap())
                        .await
                }
                _ => Err(RpcError::error(ErrKind::Unimplemented)),
            }
        }
    }

    async fn make_tcp_rpc_channel(server: &str) -> (impl AsyncRpcSender, impl AsyncRpcReceiver) {
        let mut transport = TcpStream::connect(server).await.unwrap();

        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = transport.into_split();
        (RpcSender::new(writer), RpcReceiver::new(reader))
    }

    async fn make_encrypted_tcp_rpc_channel(
        server: &str,
    ) -> (impl AsyncRpcSender, impl AsyncRpcReceiver) {
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
        let mut server = RpcServer::serve::<TcpListener>(
            srv_addr,
            service,
            false,
            Duration::from_secs(1),
            STDIOReporter::new(),
            2,
        )
        .await
        .unwrap();

        let (mut rpc_tx_1, mut rpc_rx_1) = make_tcp_rpc_channel(srv_addr).await;
        let (mut rpc_tx_2, mut rpc_rx_2) = make_tcp_rpc_channel(srv_addr).await;
        let (mut rpc_tx_3, mut rpc_rx_3) = make_tcp_rpc_channel(srv_addr).await;

        let rpc_call_1 = Message::call_with(1, &"C1").unwrap();
        let rpc_call_2 = Message::call_with(1, &"C2").unwrap();
        let rpc_call_3 = Message::call_with(1, &"C3").unwrap();

        rpc_tx_1.send(&rpc_call_1).await.unwrap();
        rpc_tx_2.send(&rpc_call_2).await.unwrap();
        rpc_tx_3.send(&rpc_call_3).await.unwrap();

        let t1 = tokio::spawn(async move {
            let reply_msg = rpc_rx_1.receive().await.unwrap();
            match reply_msg.kind {
                MessageType::Reply(reply) => {
                    let response: String = Message::decode_from_slice(&reply.data).unwrap();
                    assert!(&response == "Reply to C1");
                }
                _ => panic!("Expected reply"),
            }
        });

        let t2 = tokio::spawn(async move {
            let reply_msg = rpc_rx_2.receive().await.unwrap();
            match reply_msg.kind {
                MessageType::Reply(reply) => {
                    let response: String = Message::decode_from_slice(&reply.data).unwrap();
                    assert!(&response == "Reply to C2");
                }
                _ => panic!("Expected reply"),
            }
        });

        let t3 = tokio::spawn(async move {
            let reply_msg = rpc_rx_3.receive().await.unwrap();
            match reply_msg.kind {
                MessageType::Reply(reply) => {
                    let response: String = Message::decode_from_slice(&reply.data).unwrap();
                    assert!(&response == "Reply to C3");
                }
                _ => panic!("Expected reply"),
            }
        });

        tokio::try_join!(t1, t2, t3).expect("No panic expected");

        assert!(server.sessions() == 3);
        assert!(Arc::strong_count(&server.state) == 5);

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
        let mut server = RpcServer::serve::<TcpListener>(
            srv_addr,
            service,
            true,
            Duration::from_secs(1),
            STDIOReporter::new(),
            2,
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

        let call_msg = Message::call_with(1, &"C1").unwrap();
        rpc_tx.send(&call_msg).await.unwrap();

        let reply_msg = rpc_rx.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = Message::decode_from_slice(&reply.data).unwrap();
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
        let mut server = RpcServer::serve::<UnixListener>(
            path,
            service,
            false,
            Duration::from_secs(1),
            STDIOReporter::new(),
            2,
        )
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

        let call_msg = Message::call_with(1, &"C1").unwrap();
        rpc_tx.send(&call_msg).await.unwrap();

        let reply_msg = rpc_rx.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = Message::decode_from_slice(&reply.data).unwrap();
                assert!(&response == "Reply to C1");
            }
            _ => panic!("Expected reply"),
        }

        server.shutdown().await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
