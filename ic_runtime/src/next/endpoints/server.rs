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

use tokio::task::JoinHandle;
use tokio::time::timeout;

use pin_project_lite::pin_project;

use crate::next::codec::decoder::Decoder;
use crate::next::codec::encode::Encode;
use crate::next::codec::encoder::Encoder;
use crate::next::coop::executors;
use crate::next::coop::sync::{DynamicLatch, IList, INode, NOOP_WAKER};
use crate::next::coop::traits::{ControlHandle, Executor, Timer};
use crate::next::endpoints::application::{Application, CallContext};
use crate::next::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::next::mem::MemoryProvider;
use crate::next::reports::traits::Reporter;
use crate::next::transport::traits::{Transport, TransportInitiator, TransportServer};
use crate::next::types::core::ProtocolType;
use crate::next::types::limits::TypeLimits;
use crate::next::types::message::TypeMessageHeader;

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

    fn attach<'a>(&'a self, task: &'a mut Task) -> Option<AttachedTask<'a>> {
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

struct AttachedTask<'a> {
    tasks: &'a Tasks,
    task: &'a mut Task,
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
        I: ProtocolType + TypeLimits<Limits = ()>,
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

struct ServerState<E, P, H, R> {
    provider: P,
    tasks: Tasks,
    application: H,
    reporter: R,
    timeout: Duration,
    executor: E,
}

impl<E, P, H, R> ServerState<E, P, H, R> {
    #[inline]
    fn new(
        executor: E,
        provider: P,
        application: H,
        reporter: R,
        shards: usize,
        timeout: Duration,
    ) -> ServerState<E, P, H, R> {
        ServerState {
            provider,
            tasks: Tasks::new(shards),
            application,
            reporter,
            timeout,
            executor,
        }
    }
}

/// A multi-client server implementation.
pub struct MultiClientServer<S, E, P, H, R>
where
    E: Executor,
{
    state: Arc<ServerState<E, P, H, R>>,
    listener: E::ControlHandle<()>,
    _s: PhantomData<S>,
}

impl<S, E, P, H, R> MultiClientServer<S, E, P, H, R>
where
    S: TransportServer + Send + 'static,
    S::Initiator: Send,
    S::Transport: Send,
    E: Executor + Send + Sync + 'static + Clone,
    P: MemoryProvider<SendSegment = <S::Transport as Transport>::SendSegment>
        + Send
        + Sync
        + 'static,
    P: MemoryProvider<ReceiveSegment = <S::Transport as Transport>::ReceiveSegment>,
    P::SendSegment: Encoder + Send,
    P::ReceiveSegment: Decoder + Send,
    S::ID: Debug + Send + Sync,
    H: Application + Send + Sync + Clone + 'static,
    R: Reporter + Send + Sync + 'static,
{
    pub async fn start(
        transport_server: S,
        executor: E,
        application: H,
        provider: P,
        reporter: R,
        shards: usize,
        connection_timeout: Duration,
    ) -> ProtocolResult<Self> {
        let state = Arc::new(ServerState::new(
            executor,
            provider,
            application,
            reporter,
            shards,
            connection_timeout,
        ));

        let server_state = state.clone();
        let listener = state.executor.spawn(async move {
            loop {
                match transport_server.accept().await {
                    Ok((initiator, peer_id)) => {
                        let state = server_state.clone();
                        server_state.executor.spawn(async move {
                            // Safety:
                            // - The task and its control state are stored on the future and valid only as long
                            //   the future is still alive.
                            // - The address of the task is "assumed" to be stable,
                            //   because futures are constructed as "pinned" state machines.
                            // - Updating the task's node and accessing its data can be concurrent.
                            // - The state is atomic, updating and canceling can be concurrent.
                            let mut pinned_task = Task::new();

                            // Detached on drop with release effect.
                            if let Some(attached) = state.tasks.attach(&mut pinned_task) {
                                match E::Timer::timeout(state.timeout, initiator.initiate()).await {
                                    Ok(init_result) => match init_result {
                                        Ok(mut transport) => {
                                            Self::session(
                                                &attached,
                                                &state,
                                                &peer_id,
                                                &mut transport,
                                            )
                                            .await
                                        }
                                        Err(err) => state.reporter.error(
                                            "Failed to start session",
                                            &format_args!("{err}. Peer: {peer_id:?}"),
                                        ),
                                    },
                                    Err(_) => state.reporter.error(
                                        "Connection timeout",
                                        &format_args!("Peer: {peer_id:?}"),
                                    ),
                                }

                                // Detaching again is safe, but we try to avoid the "thundering herd" problem.
                                // This allows shutdown to access locks smoothly without contention.
                                if attached.task.node.is_canceled() {
                                    attached.release_undetached();
                                    state.reporter.trace(
                                        "Session canceled by shutdown",
                                        &format_args!("Peer: {peer_id:?}"),
                                    );
                                }
                            }
                        });
                    }
                    Err(err) => server_state
                        .reporter
                        .error("Failed to accept connection", &err),
                }
            }
        });

        Ok(Self {
            state,
            listener,
            _s: PhantomData,
        })
    }

    async fn session(
        task: &AttachedTask<'_>,
        state: &ServerState<E, P, H, R>,
        peer_id: &S::ID,
        transport: &mut S::Transport,
    ) {
        let reporter = &state.reporter;
        let application = state.application.clone();
        let provider = &state.provider;

        loop {
            // TODO: Refine the allocation strategy server-wide and the scope of leasing decoding-segments.
            let Some(mut recv_segment) = provider.acquire_receive() else {
                reporter.error(
                    "Failed to get memory for receiving",
                    &format_args!("Peer: {peer_id:?}"),
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
                // TODO: Matching according to id and directive-rules.
                Ok((id, directive)) => {
                    let mut context = ServerContext::new(provider, transport, id);

                    if let Err(err) = application
                        .call(directive, recv_segment, &mut context)
                        .await
                    {
                        reporter.error(
                            "Application failed to process the message",
                            &format_args!("{err}. Peer: {peer_id:?}"),
                        );
                        return;
                    }
                }
                Err(err) => {
                    reporter.error(
                        "Failed to decode the header of received message",
                        &format_args!("{err}. Peer: {peer_id:?}"),
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

    /// Shutdowns the server and the application in planned mode.
    ///
    /// This call doesn't have immediate effect and may take longer time,
    /// because it allows active sessions to complete processing the current received message.
    pub async fn terminate(&mut self) -> ProtocolResult<()> {
        self.state.tasks.observer.open();
        self.listener.abort();

        for shard in &self.state.tasks.shards {
            shard.lock().drain(|task| task.cancel());
        }

        self.state.tasks.observer.wait().await;

        self.state.application.terminate().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::next::coop::executors::TokioExecutor;
    use crate::next::mem::{IOPool, IOSegment};
    use crate::next::transport::stream::uds::{UnixLink, UnixLinkServer};
    use crate::next::transport::traits::Transport;
    use crate::next::types::core::TypeU64;

    #[derive(Clone)]
    struct TestApplication;

    impl Application for TestApplication {
        async fn call<E, M, C>(&self, op: u64, message: M, context: &mut C) -> ProtocolResult<()>
        where
            E: Encoder,
            M: Decoder + Send,
            C: CallContext<E> + Send,
        {
            if op == 1 {
                let value = message.decode::<TypeU64>(())?.0;
                context
                    .respond_with::<TypeU64, TypeU64>(1, &TypeU64(value + 1))
                    .await?;
                Ok(())
            } else {
                Err(ProtocolError::error(ErrKind::Unimplemented))
            }
        }

        async fn call_nullary<E, C>(&self, _op: u64, _context: &mut C) -> ProtocolResult<()>
        where
            E: Encoder,
            C: CallContext<E> + Send,
        {
            Err(ProtocolError::error(ErrKind::Unimplemented))
        }

        async fn terminate(&self) -> ProtocolResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_multi_client_server() {
        let path = "/tmp/test_multi_client_server.sock";
        let _ = std::fs::remove_file(path);

        let pool = IOPool::new(2, 32);
        let application = TestApplication;
        let executor = TokioExecutor;
        let reporter = ();

        let transport_server = UnixLinkServer::create(&path).await.unwrap();
        let server_provider = pool.clone();
        let mut server = MultiClientServer::start(
            transport_server,
            executor,
            application,
            server_provider,
            reporter,
            1,
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        // Connect client.
        let mut client_transport = UnixLink::connect(&path).await.unwrap();

        // Send.
        let mut segment = pool.acquire().unwrap();
        TypeMessageHeader::encode_header(123, 1, &mut segment).unwrap();
        segment.encode_next(&TypeU64(100), ()).unwrap();
        client_transport.send(&mut segment).await.unwrap();

        // Receive.
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
