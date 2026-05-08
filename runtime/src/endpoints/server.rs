use core::cell::{Cell, UnsafeCell};
use core::fmt::Debug;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::ManuallyDrop;
use core::pin::Pin;
use core::sync::atomic::AtomicU8;
use core::sync::atomic::Ordering::{AcqRel, Acquire};
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use std::sync::Arc;

use pin_project_lite::pin_project;

use crate::codec::decoder::Decoder;
use crate::codec::encode::Encode;
use crate::codec::encoder::Encoder;
use crate::codec::types::core::ProtocolType;
use crate::codec::types::message::TypeMessageHeader;
use crate::concurrency::server::traits::{Task, TaskServer, Timer};
use crate::concurrency::sync::{DynamicLatch, IList, INode, NOOP_WAKER};
use crate::endpoints::service::{CallContext, Session, SessionServer};
use crate::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::mem::MemoryProvider;
use crate::reports::traits::Reporter;
use crate::transport::traits::{Transport, TransportInitiator, TransportServer};

thread_local! {
    // Must be non-zero.
    static RNG_STATE: Cell<u64> = const { Cell::new(0x12345678ABCDEF) };
}

struct Tasks {
    observer: DynamicLatch,
    shards: Box<[parking_lot::Mutex<IList<TaskControlState>>]>,
    shards_mask: usize,
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
            observer: DynamicLatch::new(),
            shards: shards.into_boxed_slice(),
            shards_mask: n_shards - 1,
        }
    }

    /// Selects a shard within the range of allocated shards randomly.
    ///
    /// The algorithm has distribution property to prevent clustering.
    fn select_shard(&self) -> usize {
        RNG_STATE.with(|s| {
            let mut x = s.get();
            // xorshift64 star.
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            s.set(x);
            (x.wrapping_mul(0x2545F4914F6CDD1D) as usize) & self.shards_mask
        })
    }

    fn attach<'a>(&'a self, task: &'a mut TaskControl) -> Option<AttachedTask<'a>> {
        if !self.observer.acquire_manual() {
            return None;
        };

        let shard = self.select_shard();

        unsafe {
            let mut shard_lock = self.shards[shard].lock();
            shard_lock.attach_first(&mut task.node);
            drop(shard_lock);
        };

        Some(AttachedTask {
            task,
            tasks: self,
            shard,
        })
    }

    fn detach(&self, task: &mut AttachedTask<'_>) {
        unsafe { self.shards[task.shard].lock().detach(&mut task.task.node) };
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

struct TaskControl {
    node: INode<TaskControlState>,
}

unsafe impl Send for TaskControl {}
unsafe impl Sync for TaskControl {}

impl TaskControl {
    #[inline(always)]
    const fn new() -> Self {
        Self {
            node: INode::new(TaskControlState::new()),
        }
    }
}

struct AttachedTask<'a> {
    tasks: &'a Tasks,
    task: &'a mut TaskControl,
    shard: usize,
}

impl<'a> Drop for AttachedTask<'a> {
    fn drop(&mut self) {
        self.tasks.detach(self);
        self.tasks.observer.release();
    }
}

impl<'a> AttachedTask<'a> {
    #[inline(always)]
    fn wait_cancelable<F>(&self, future: F) -> CancelableTask<'_, F> {
        CancelableTask {
            control: &self.task.node,
            future,
        }
    }

    #[inline(always)]
    fn release_undetached(self) {
        self.tasks.observer.release();
        let _ = ManuallyDrop::new(self);
    }
}

struct ServerContext<'a, P, T> {
    provider: &'a P,
    transport: &'a mut T,
    id: u64,
}

impl<'a, P, T> ServerContext<'a, P, T> {
    #[inline(always)]
    const fn new(provider: &'a P, transport: &'a mut T, id: u64) -> Self {
        Self {
            provider,
            transport,
            id,
        }
    }
}

impl<'a, P, T> CallContext<P::SendSegment> for ServerContext<'a, P, T>
where
    T: Transport + Send,
    T::SendSegment: Send,
    P: MemoryProvider<SendSegment = T::SendSegment> + Sync,
    P::SendSegment: Encoder,
{
    type CallID = u64;

    fn call_id(&self) -> &Self::CallID {
        &self.id
    }

    async fn respond_with<'c, I, R>(&mut self, op: u64, response: &'c R) -> ProtocolResult<()>
    where
        I: ProtocolType<Limits = ()>,
        R: Sync,
        &'c R: Encode<I, P::SendSegment>,
    {
        let Some(mut send_segment) = self.provider.acquire_send() else {
            return Err(ProtocolError::error(ErrKind::CapacityLimit));
        };

        TypeMessageHeader::encode_header(self.id, op, &mut send_segment)?;
        send_segment.encode_next(response, ())?;

        self.transport.send(&mut send_segment).await
    }
}

struct ServerState<E, M, S, R> {
    provider: M,
    tasks: Tasks,
    session_server: S,
    reporter: R,
    timeout: Duration,
    task_server: E,
}

impl<E, P, H, R> ServerState<E, P, H, R> {
    #[inline]
    fn new(
        task_server: E,
        provider: P,
        session_server: H,
        reporter: R,
        shards: usize,
        timeout: Duration,
    ) -> ServerState<E, P, H, R> {
        ServerState {
            provider,
            tasks: Tasks::new(shards),
            session_server,
            reporter,
            timeout,
            task_server,
        }
    }
}

/// A multi-client server implementation.
pub struct MultiClientServer<S, E, P, H, R>
where
    E: TaskServer,
{
    state: Arc<ServerState<E, P, H, R>>,
    server_task: E::Task<()>,
    _s: PhantomData<S>,
}

impl<T, E, M, S, R> MultiClientServer<T, E, M, S, R>
where
    T: TransportServer + Send + 'static,
    T::Initiator: Send,
    T::Transport: Send,
    T::Info: Debug + Send + Sync,
    // Note: ID = () because identification is unsupported currently.
    S: SessionServer<()> + Send + Sync + 'static,
    for<'a> S::Session<'a>: Send,
    M: MemoryProvider<SendSegment = <T::Transport as Transport>::SendSegment>
        + Send
        + Sync
        + 'static,
    M: MemoryProvider<ReceiveSegment = <T::Transport as Transport>::ReceiveSegment>,
    M::SendSegment: Encoder + Send,
    M::ReceiveSegment: Decoder + Send,
    E: TaskServer + Send + Sync + 'static + Clone,
    R: Reporter + Send + Sync + 'static,
{
    pub async fn start(
        transport_server: T,
        session_server: S,
        provider: M,
        task_server: E,
        reporter: R,
        shards: usize,
        connection_timeout: Duration,
    ) -> ProtocolResult<Self> {
        let state = Arc::new(ServerState::new(
            task_server,
            provider,
            session_server,
            reporter,
            shards,
            connection_timeout,
        ));

        let server_task = state
            .task_server
            .create(Self::server_task(state.clone(), transport_server));

        Ok(Self {
            state,
            server_task,
            _s: PhantomData,
        })
    }

    #[inline]
    async fn server_task(state: Arc<ServerState<E, M, S, R>>, transport_server: T) {
        loop {
            match transport_server.accept().await {
                Ok((initiator, peer_info)) => {
                    state.task_server.create(Self::connection_task(
                        state.clone(),
                        initiator,
                        peer_info,
                    ));
                }
                Err(err) => state.reporter.error("Failed to accept connection", &err),
            }
        }
    }

    #[inline]
    async fn connection_task(
        state: Arc<ServerState<E, M, S, R>>,
        initiator: T::Initiator,
        peer_info: T::Info,
    ) {
        // Safety:
        // - The task and its control state are stored on the future and valid only as long
        //   the future is still alive.
        // - The address of the task is "assumed" to be stable,
        //   because futures are constructed as "pinned" state machines.
        // - Updating the task's node and accessing its data can be concurrent.
        // - The state is atomic, updating and canceling can be concurrent.
        let mut pinned_task = TaskControl::new();

        // Detached on drop with release effect.
        if let Some(attached) = state.tasks.attach(&mut pinned_task) {
            match E::Timer::timeout(state.timeout, initiator.initiate()).await {
                Ok(init_result) => match init_result {
                    Ok(mut transport) => {
                        Self::session(&attached, &state, &peer_info, &mut transport).await
                    }
                    Err(err) => state.reporter.error(
                        "Failed to start session",
                        &format_args!("{err}. Peer: {peer_info:?}"),
                    ),
                },
                Err(_) => state
                    .reporter
                    .info("Connection timeout", &format_args!("Peer: {peer_info:?}")),
            }

            // Detaching again is safe, but we try to avoid the "thundering herd" problem.
            // This allows shutdown to access locks smoothly without contention.
            if attached.task.node.is_canceled() {
                attached.release_undetached();
                state.reporter.trace(
                    "Session canceled by shutdown",
                    &format_args!("Peer: {peer_info:?}"),
                );
            }
        }
    }

    async fn session(
        task: &AttachedTask<'_>,
        state: &ServerState<E, M, S, R>,
        peer_info: &T::Info,
        transport: &mut T::Transport,
    ) {
        let reporter = &state.reporter;
        let provider = &state.provider;
        let session = state.session_server.create(());

        loop {
            // TODO: Refine the allocation strategy server-wide.
            let Some(mut recv_segment) = provider.acquire_receive() else {
                reporter.error(
                    "Failed to get memory for receiving",
                    &format_args!("Peer: {peer_info:?}"),
                );
                return;
            };

            if let Err(Canceled) = task
                .wait_cancelable(transport.receive(&mut recv_segment))
                .await
            {
                return;
            }

            match TypeMessageHeader::decode_header(&mut recv_segment) {
                // TODO: Matching according to the rules of both the id and the directive.
                Ok((id, directive)) => {
                    let mut context = ServerContext::new(provider, transport, id);

                    if let Err(err) = session.call(directive, recv_segment, &mut context).await {
                        reporter.error(
                            "Service failed to process the message",
                            &format_args!("{err}. Peer: {peer_info:?}"),
                        );
                        return;
                    }
                }
                Err(err) => {
                    reporter.error(
                        "Failed to decode the header of received message",
                        &format_args!("{err}. Peer: {peer_info:?}"),
                    );
                    return;
                }
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
    pub async fn terminate(&mut self) -> ProtocolResult<()> {
        self.state.tasks.observer.open();
        self.server_task.cancel();

        for shard in &self.state.tasks.shards {
            shard.lock().drain(|task| task.cancel());
        }

        self.state.tasks.observer.wait().await;

        self.state.session_server.terminate().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::types::core::TypeU64;
    use crate::concurrency::server::tokio::TokioServer;
    use crate::mem::{IOPool, IOSegment};
    use crate::transport::stream::uds::{UnixLink, UnixLinkServer};
    use crate::transport::traits::Transport;

    /// Theoretical definition:
    ///  ```
    /// message TestMessage {
    ///     value: u64
    /// }
    ///
    /// interface TestService {
    ///   increment_one(TestMessage): TestMessage;
    /// }
    /// ```
    struct TestMessage {
        value: TypeU64,
    }

    impl TestMessage {
        fn new(value: u64) -> Self {
            Self {
                value: TypeU64(value),
            }
        }
    }

    trait TestService {
        fn increment_one(&self, message: TestMessage) -> impl Future<Output = TestMessage> + Send;
    }

    impl TestService for () {
        async fn increment_one(&self, message: TestMessage) -> TestMessage {
            TestMessage::new(message.value + 1)
        }
    }

    struct TestSession<'a, S: TestService> {
        svc_def: S,
        _svc_lt: PhantomData<&'a ()>,
    }

    impl<'a, S: TestService> TestSession<'a, S> {
        fn new(svc_def: S) -> Self {
            Self {
                svc_def,
                _svc_lt: PhantomData,
            }
        }
    }

    impl<'a, S> Session<'a> for TestSession<'a, S>
    where
        S: TestService + Sync,
    {
        async fn call<E, M, C>(&self, op: u64, message: M, context: &mut C) -> ProtocolResult<()>
        where
            E: Encoder,
            M: Decoder + Send,
            C: CallContext<E> + Send,
        {
            match op {
                1 => {
                    let value = message.decode::<TypeU64>(())?.0;

                    let svc_response = self.svc_def.increment_one(TestMessage::new(value)).await;

                    context.respond_with(1, &svc_response.value).await
                }
                _ => Err(ProtocolError::error(ErrKind::Unimplemented)),
            }
        }

        async fn call_nullary<E, C>(&self, _op: u64, _context: &mut C) -> ProtocolResult<()>
        where
            E: Encoder,
            C: CallContext<E> + Send,
        {
            Err(ProtocolError::error(ErrKind::Unimplemented))
        }
    }

    struct TestSessionServer;

    impl<T> SessionServer<T> for TestSessionServer {
        type Session<'a> = TestSession<'a, ()>;

        fn create<'a>(&'a self, _id: T) -> Self::Session<'a> {
            TestSession::new(())
        }

        async fn terminate(&self) -> ProtocolResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_multi_client_server() {
        let path = "/tmp/test_multi_client_server.sock";
        let _ = std::fs::remove_file(path);

        let memory_provider = IOPool::new(2, 32);
        let transport_server = UnixLinkServer::create(&path).await.unwrap();
        let session_server = TestSessionServer;
        let task_server = TokioServer;
        let reporter = ();

        let memory_server = memory_provider.clone();

        let mut server = MultiClientServer::start(
            transport_server,
            session_server,
            memory_server,
            task_server,
            reporter,
            1,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let mut client_transport = UnixLink::connect(&path).await.unwrap();

        let mut segment = memory_provider.acquire().unwrap();

        TypeMessageHeader::encode_header(123, 1, &mut segment).unwrap();
        segment.encode_next(&TypeU64(100), ()).unwrap();
        client_transport.send(&mut segment).await.unwrap();

        segment.clear();
        client_transport.receive(&mut segment).await.unwrap();

        let (id, op) = TypeMessageHeader::decode_header(&mut segment).unwrap();
        assert_eq!(id, 123);
        assert_eq!(op, 1);

        let response = segment.decode::<TypeU64>(()).unwrap();
        assert_eq!(response.0, 101);

        server.terminate().await.unwrap();
        let _ = std::fs::remove_file(path);
    }
}
