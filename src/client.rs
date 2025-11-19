use std::cell::UnsafeCell;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::{mem, ptr};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use serde::{Deserialize, Serialize};

use crate::application::{Call, CallContext, RpcApplication};
use crate::core::{
    AsyncReceiver, AsyncSender, Directive, EncMessageReceiver, EncMessageSender, Message,
    MessageID, MessageReceiver, MessageSender, MessageStore,
};
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::report::Reporter;
use crate::specs::{RpcSpecification, negotiation};
use crate::sync::{DynamicLatch, NOOP_WAKER};
use crate::transport::TransportLayer;

/// The common RPC client interface of async clients.
pub trait AsyncRpcClient {
    fn call<P, R>(&self, op: u16, params: &P) -> impl Future<Output = RpcResult<R>>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>;

    fn call_timeout<P, R>(
        &self,
        op: u16,
        params: &P,
        timeout: Duration,
    ) -> impl Future<Output = RpcResult<R>>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>;

    fn call_one_way<P>(&self, op: u16, params: &P) -> impl Future<Output = RpcResult<()>>
    where
        P: Serialize + Sync;

    fn call_nullary<R>(&self, op: u16) -> impl Future<Output = RpcResult<R>>
    where
        R: for<'de> Deserialize<'de>;

    fn call_nullary_timeout<R>(
        &self,
        op: u16,
        timeout: Duration,
    ) -> impl Future<Output = RpcResult<R>>
    where
        R: for<'de> Deserialize<'de>;

    fn call_nullary_one_way(&self, op: u16) -> impl Future<Output = RpcResult<()>>;

    fn ping(&self, timeout: Duration) -> impl Future<Output = RpcResult<()>>;

    fn terminate(&mut self) -> impl Future<Output = RpcResult<()>>;
}

struct FrameData<T> {
    state: AtomicU8,
    waker: UnsafeCell<Waker>,
    // TODO: Maybeuninit?
    value: UnsafeCell<Option<T>>,
    _pin: PhantomPinned,
}

const DATA_WAIT: u8 = 0;
const DATA_INIT: u8 = 1;
const DATA_READY: u8 = 2;

impl<T> FrameData<T> {
    #[inline(always)]
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(DATA_WAIT),
            waker: UnsafeCell::new(NOOP_WAKER),
            value: UnsafeCell::new(None),
            _pin: PhantomPinned,
        }
    }

    #[inline(always)]
    const fn take_value(&self) -> Option<T> {
        let value_ptr = self.value.get();
        unsafe { (*value_ptr).take() }
    }
}

struct WaitPublishing<'a, T> {
    frame: &'a FrameData<T>,
}

impl<'a, T> WaitPublishing<'a, T> {
    #[inline(always)]
    const fn new(frame: &'a FrameData<T>) -> Self {
        Self { frame }
    }
}

impl<'a, T> Future for WaitPublishing<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let frame = unsafe { self.get_unchecked_mut().frame };
        let state = &frame.state;

        match state.compare_exchange(DATA_WAIT, DATA_INIT, Acquire, Acquire) {
            Ok(_) => {
                let waker_ptr = frame.waker.get();
                unsafe { *waker_ptr = cx.waker().clone() };

                match state.compare_exchange(DATA_INIT, DATA_WAIT, AcqRel, Acquire) {
                    Ok(_) => return Poll::Pending,
                    Err(current) => debug_assert!(current & DATA_READY != 0),
                };
            }
            Err(current) => debug_assert!(current & DATA_READY != 0),
        }

        // Safety: DATA_READY indicates that writing has been completed.
        Poll::Ready(())
    }
}

struct FramePublisher<T> {
    data_ptr: *mut FrameData<T>,
}

unsafe impl<T: Send> Send for FramePublisher<T> {}

impl<T> FramePublisher<T> {
    #[inline(always)]
    const fn new(bus: &mut FrameData<T>) -> Self {
        Self { data_ptr: bus }
    }

    const fn null() -> Self {
        Self {
            data_ptr: ptr::null_mut(),
        }
    }

    const fn is_null(&self) -> bool {
        self.data_ptr.is_null()
    }

    #[inline]
    fn publish(&mut self, value: T) {
        let frame_data = unsafe { &mut (*self.data_ptr) };

        let value_ptr = frame_data.value.get();

        debug_assert!(unsafe { (*value_ptr).is_none() });

        unsafe { value_ptr.write(Some(value)) };

        if frame_data.state.fetch_or(DATA_READY, AcqRel) == DATA_WAIT {
            unsafe { (*frame_data.waker.get()).wake_by_ref() };
        }
    }
}

struct Slot<T> {
    next: AtomicU32,
    cycle: AtomicU32,
    guard: parking_lot::Mutex<()>,
    publisher: UnsafeCell<FramePublisher<T>>,

    #[cfg(test)]
    reserved: AtomicBool,
}

unsafe impl<T> Sync for Slot<T> {}

struct MatchingUnit<T> {
    // Practically, it is static, but const N will force full type annotation.
    slots: Box<[Slot<T>]>,
    free: AtomicU64,
}

impl<T> MatchingUnit<T> {
    const INVALID_INDEX: u32 = u32::MAX;

    fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0 && capacity <= u32::MAX as usize,
            "Capacity must be > 0 and <= u32::MAX"
        );

        let mut slots = Vec::with_capacity(capacity);

        let cap_u32 = capacity as u32;

        for i in 0..cap_u32 {
            slots.push(Slot {
                next: AtomicU32::new(i + 1),
                cycle: AtomicU32::new(0),
                guard: parking_lot::Mutex::new(()),
                publisher: UnsafeCell::new(FramePublisher::null()),

                #[cfg(test)]
                reserved: AtomicBool::new(false),
            });
        }

        slots[capacity - 1].next.store(Self::INVALID_INDEX, Relaxed);

        Self {
            slots: slots.into_boxed_slice(),
            free: AtomicU64::new(0),
        }
    }

    const fn split(id: MessageID) -> (u32, u32) {
        (id as u32, (id >> 32) as u32)
    }

    const fn combine(index: u32, tag: u32) -> u64 {
        ((tag as u64) << 32) | index as u64
    }

    fn acquire(&self, publisher: FramePublisher<T>) -> Option<SlotRef<'_, T>> {
        loop {
            let current = self.free.load(Acquire);
            let (current_index, current_tag) = Self::split(current);

            if current_index == Self::INVALID_INDEX {
                return None;
            }

            let current_slot = &self.slots[current_index as usize];
            let next_index = current_slot.next.load(Relaxed);

            let new = Self::combine(next_index, current_tag.wrapping_add(1));

            if self
                .free
                .compare_exchange(current, new, AcqRel, Relaxed)
                .is_ok()
            {
                let cycle = current_slot.cycle.load(Relaxed);

                unsafe { (*current_slot.publisher.get()) = publisher };

                let acquired = Self::combine(current_index, cycle);

                #[cfg(test)]
                current_slot.reserved.store(true, Ordering::Release);

                return Some(SlotRef::new(acquired, self));
            }
        }
    }

    /// Tries to published the value to the identified slot.
    ///
    /// If the slot is unidentified, the value is dropped.
    ///
    /// This call blocks access to the releasing path.
    fn publish(&self, id: MessageID, value: T) {
        let (index, prev_cycle) = Self::split(id);
        let index_usize = index as usize;

        if index_usize >= self.slots.len() {
            return;
        }

        let slot = &self.slots[index_usize];

        // Failure to acquire a lock means cancelling has been started.
        if let Some(_lock) = slot.guard.try_lock() {
            // Safety: lock until release finishes.

            let current_cycle = slot.cycle.load(Relaxed);

            if current_cycle == prev_cycle {
                unsafe {
                    let publisher = &mut (*slot.publisher.get());
                    // RT_ASSERT
                    // Guard against accidental matching.
                    assert!(!publisher.is_null());
                    publisher.publish(value);
                    self.unconditional_release(index, prev_cycle)
                };
            }
        }
    }

    /// Tries to cancel in-flight issue.
    ///
    /// This call blocks or gets blocked by overlapping access to the releasing path.
    fn cancel(&self, id: MessageID) {
        let (index, prev_cycle) = Self::split(id);

        let slot = &self.slots[index as usize];

        // Safety: block or get blocked until release finishes.
        let _release_lock = slot.guard.lock();

        let current_cycle = slot.cycle.load(Relaxed);

        if current_cycle == prev_cycle {
            unsafe { self.unconditional_release(index, prev_cycle) };
        }
    }

    /// Releases the slot at the provided index by making it available for ownership.
    ///
    /// Safety: This call doesn't verify the ownership of the slot which might be still in use.
    unsafe fn unconditional_release(&self, index: u32, cycle: u32) {
        let slot = &self.slots[index as usize];

        let new_cycle = cycle.wrapping_add(1);

        slot.cycle.store(new_cycle, Relaxed);

        // Guard against accidental matching.
        unsafe { ptr::write(slot.publisher.get(), FramePublisher::null()) }

        #[cfg(test)]
        slot.reserved.store(false, Ordering::Release);

        loop {
            let current = self.free.load(Acquire);
            let (current_index, current_tag) = Self::split(current);

            slot.next.store(current_index, Relaxed);

            let new = Self::combine(index, current_tag.wrapping_add(1));

            if self
                .free
                .compare_exchange(current, new, AcqRel, Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }
}

pub struct SlotRef<'a, T> {
    id: MessageID,
    mu: &'a MatchingUnit<T>,
}

impl<'a, T> SlotRef<'a, T> {
    #[inline(always)]
    const fn new(id: MessageID, mu: &'a MatchingUnit<T>) -> Self {
        Self { id, mu }
    }

    #[inline(always)]
    pub fn forget(self) {
        mem::forget(self);
    }
}

impl<'a, T> Drop for SlotRef<'a, T> {
    fn drop(&mut self) {
        self.mu.cancel(self.id);
    }
}

enum Response {
    Pong,
    Data(MessageStore),
}

struct ClientState<S, H, E> {
    abort_lock: DynamicLatch,
    entries: MatchingUnit<RpcResult<Response>>,
    sender: Mutex<S>,
    app: H,
    reporter: E,
}

impl<S, H, E> ClientState<S, H, E> {
    #[inline(always)]
    fn new(sender: S, capacity: usize, reporter: E, app: H) -> ClientState<S, H, E> {
        ClientState {
            abort_lock: DynamicLatch::new(),
            entries: MatchingUnit::new(capacity),
            sender: Mutex::const_new(sender),
            app,
            reporter,
        }
    }
}

struct ClientContext<'a, S, H, E> {
    id: &'a MessageID,
    state: &'a ClientState<S, H, E>,
}

impl<'a, S, H, E> ClientContext<'a, S, H, E>
where
    S: AsyncSender + Send,
{
    #[inline(always)]
    const fn new(id: &'a MessageID, sender: &'a ClientState<S, H, E>) -> Self {
        Self { id, state: sender }
    }
}

impl<'a, S, H, E> CallContext for ClientContext<'a, S, H, E>
where
    S: AsyncSender + Send,
    H: RpcApplication + Sync,
    E: Reporter + Sync,
{
    type ID = MessageID;

    #[inline(always)]
    fn id(&self) -> &Self::ID {
        self.id
    }

    #[inline]
    async fn return_data<R: Serialize + Sync>(&mut self, reply: &R) -> RpcResult<()> {
        self.state
            .sender
            .lock()
            .await
            .return_data(self.id, reply)
            .await
    }

    #[inline]
    async fn return_error(&mut self, err: RpcError) -> RpcResult<()> {
        self.state
            .sender
            .lock()
            .await
            .return_error(self.id, err)
            .await
    }

    #[inline]
    async fn call<P: Serialize + Sync>(&mut self, op: u16, params: &P) -> RpcResult<()> {
        self.state
            .sender
            .lock()
            .await
            .call(self.id, op, &params)
            .await
    }

    #[inline]
    async fn call_nullary(&mut self, op: u16) -> RpcResult<()> {
        self.state
            .sender
            .lock()
            .await
            .call_nullary(self.id, op)
            .await
    }
}

/// An async RPC client implementation.
/// This implementation utilizes single shared transport instance,
/// which makes it very lightweight at the cost of some synchronization overhead.
pub struct RpcAsyncClient<S, H, E> {
    state: Arc<ClientState<S, H, E>>,
    recv_task: JoinHandle<()>,
}

// Core private implementation.
impl<S, H, E> RpcAsyncClient<S, H, E>
where
    S: AsyncSender + Send + 'static,
    H: RpcApplication + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    fn init<R>(
        capacity: usize,
        sender: S,
        mut receiver: R,
        reporter: E,
        handler: H,
    ) -> RpcAsyncClient<S, H, E>
    where
        S: AsyncSender + Send + 'static,
        R: AsyncReceiver + Send + Sync + 'static,
    {
        let state = Arc::new(ClientState::new(sender, capacity, reporter, handler));
        let client_state = Arc::clone(&state);

        let recv_task = tokio::spawn(async move {
            let reporter = &client_state.reporter;
            loop {
                match receiver.receive().await {
                    Ok(_) => {
                        if let Err(err) = Self::process_message(&receiver, &client_state).await {
                            reporter.error("Processing error", &err);
                            break;
                        }
                    }
                    Err(err) => {
                        reporter.error("Receiving error", &err);
                        break;
                    }
                }
            }
        });

        Self { state, recv_task }
    }

    async fn process_message<R>(receiver: &R, state: &Arc<ClientState<S, H, E>>) -> RpcResult<()>
    where
        R: AsyncReceiver,
    {
        let message = receiver.message();
        let header = Message::decode_header(message)?;
        match header.directive {
            Directive::Return => {
                let data = Message::returned_data(message)?;
                let mut ret = MessageStore::with_capacity(data.len());
                unsafe { ret.copy_from(data) };
                state.entries.publish(header.id, Ok(Response::Data(ret)));
            }
            Directive::Error => {
                let err = Message::decode_error(message)?;
                state.entries.publish(header.id, Err(err));
            }
            Directive::Pong => {
                state.entries.publish(header.id, Ok(Response::Pong));
            }
            Directive::Call => {
                if let Some(_lock) = state.abort_lock.acquire() {
                    let (op, params) = Message::decode_op_return_params(message)?;
                    let mut context = ClientContext::new(&header.id, state);
                    return state.app.call(Call { op, params }, &mut context).await;
                }
            }
            Directive::NullaryCall => {
                if let Some(_lock) = state.abort_lock.acquire() {
                    let op = Message::decode_op(message)?;
                    let mut context = ClientContext::new(&header.id, state);
                    return state.app.call_nullary(op, &mut context).await;
                }
            }
            Directive::Ping => return state.sender.lock().await.pong(&header.id).await,
        }
        Ok(())
    }
}

impl<T, H, E> RpcAsyncClient<MessageSender<T>, H, E>
where
    T: TransportLayer + 'static,
    H: RpcApplication + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    pub async fn connect(
        capacity: usize,
        mut transport: T,
        reporter: E,
        application: H,
    ) -> RpcResult<RpcAsyncClient<MessageSender<T::OwnedWriteHalf>, H, E>> {
        negotiation::initiate(&mut transport, RpcSpecification::new(1, false)).await?;

        let (r, w) = transport.into_split();

        let instance = RpcAsyncClient::init(
            capacity,
            MessageSender::new(w),
            MessageReceiver::new(r),
            reporter,
            application,
        );

        Ok(instance)
    }
}

impl<T, H, E> RpcAsyncClient<EncMessageSender<T>, H, E>
where
    T: TransportLayer + 'static,
    H: RpcApplication + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    pub async fn connect_encrypted(
        capacity: usize,
        mut transport: T,
        reporter: E,
        application: H,
    ) -> RpcResult<RpcAsyncClient<EncMessageSender<T::OwnedWriteHalf>, H, E>> {
        negotiation::initiate(&mut transport, RpcSpecification::new(1, true)).await?;

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut transport).await?;

        let (r, w) = transport.into_split();

        let instance = RpcAsyncClient::init(
            capacity,
            EncMessageSender::new(w, w_key),
            EncMessageReceiver::new(r, r_key),
            reporter,
            application,
        );

        Ok(instance)
    }
}

impl<S, H, E> AsyncRpcClient for RpcAsyncClient<S, H, E>
where
    S: AsyncSender + Send + 'static,
    H: RpcApplication + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    /// Makes a remote procedure call.
    /// Default timeout is `30` seconds.
    async fn call<P, R>(&self, op: u16, params: &P) -> RpcResult<R>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>,
    {
        self.call_timeout(op, params, Duration::from_secs(30)).await
    }

    /// Makes a remote procedure call with custom timeout.
    async fn call_timeout<P, R>(&self, op: u16, params: &P, timeout: Duration) -> RpcResult<R>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>,
    {
        // Safety: This value must not move.
        let mut pinned_mem = FrameData::new();

        let entries = &self.state.entries;

        if let Some(slot) = entries.acquire(FramePublisher::new(&mut pinned_mem)) {
            self.state
                .sender
                .lock()
                .await
                .call(&slot.id, op, params)
                .await?;

            tokio::time::timeout(timeout, WaitPublishing::new(&pinned_mem)).await?;

            slot.forget();

            // RT_ASSERT
            let published = pinned_mem.take_value().unwrap();

            match published {
                Ok(response) => {
                    if let Response::Data(reply) = response {
                        return Message::decode_from_slice(reply.data());
                    }
                    return Err(RpcError::error(ErrKind::UnexpectedMsg));
                }
                Err(err) => return Err(err),
            }
        }

        Err(RpcError::error(ErrKind::CapacityLimit))
    }

    /// Sends a one-way call without response.
    ///
    /// This call is untracked, if the target operation returns response,
    /// the response will be discarded.
    async fn call_one_way<P>(&self, op: u16, params: &P) -> RpcResult<()>
    where
        P: Serialize + Sync,
    {
        self.state.sender.lock().await.call(&0, op, params).await
    }

    /// Makes a remote procedure call.
    /// Default timeout is `30` seconds.
    async fn call_nullary<R>(&self, op: u16) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        self.call_nullary_timeout(op, Duration::from_secs(30)).await
    }

    /// Makes a remote procedure call with custom timeout.
    async fn call_nullary_timeout<R>(&self, op: u16, timeout: Duration) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        // Safety: This value must not move.
        let mut pinned_mem = FrameData::new();

        let entries = &self.state.entries;

        if let Some(slot) = entries.acquire(FramePublisher::new(&mut pinned_mem)) {
            self.state
                .sender
                .lock()
                .await
                .call_nullary(&slot.id, op)
                .await?;

            tokio::time::timeout(timeout, WaitPublishing::new(&pinned_mem)).await?;

            slot.forget();

            // RT_ASSERT
            let published = pinned_mem.take_value().unwrap();

            match published {
                Ok(response) => {
                    if let Response::Data(reply) = response {
                        return Message::decode_from_slice(reply.data());
                    }
                    return Err(RpcError::error(ErrKind::UnexpectedMsg));
                }
                Err(err) => return Err(err),
            }
        }

        Err(RpcError::error(ErrKind::CapacityLimit))
    }

    /// Sends a one-way nullary call without response.
    ///
    /// This call is untracked, if the target operation returns response,
    /// the response will be discarded.
    async fn call_nullary_one_way(&self, op: u16) -> RpcResult<()> {
        self.state.sender.lock().await.call_nullary(&0, op).await
    }

    /// Sends a `ping`` message.
    async fn ping(&self, timeout: Duration) -> RpcResult<()> {
        // Safety: This value must not move.
        let mut pinned_mem = FrameData::new();

        let entries = &self.state.entries;

        if let Some(slot) = entries.acquire(FramePublisher::new(&mut pinned_mem)) {
            self.state.sender.lock().await.ping(&slot.id).await?;

            tokio::time::timeout(timeout, WaitPublishing::new(&pinned_mem)).await?;

            slot.forget();

            return Ok(());
        }

        Err(RpcError::error(ErrKind::CapacityLimit))
    }

    /// Closes its sender and shutdowns the receiving task in graceful manner.
    ///
    /// This call doesn't have immediate effect and may take longer time,
    /// because it allows critical regions to fully complete their execution.
    ///
    /// Buffered data will be sent followed by FIN message.
    ///
    /// Any attempts to send messages after this call will return `Broken pipe` I/O error.
    async fn terminate(&mut self) -> RpcResult<()> {
        self.state.abort_lock.open();

        self.state.abort_lock.wait().await;

        self.recv_task.abort();

        self.state.sender.lock().await.terminate().await?;

        self.state.app.terminate().await
    }
}

#[cfg(test)]
mod tests_matching_unit {
    use super::*;

    use std::thread;

    fn debug_integrity<T>(mu: &MatchingUnit<T>) -> usize {
        let mut seen = vec![false; mu.slots.len()];

        let current_free = mu.free.load(Relaxed);
        let (mut index, _) = MatchingUnit::<T>::split(current_free);

        let mut count = 0;

        while index != MatchingUnit::<T>::INVALID_INDEX {
            if seen[index as usize] {
                panic!("Cycle detected");
            }

            seen[index as usize] = true;

            let slot = &mu.slots[index as usize];
            if slot.reserved.load(Relaxed) {
                panic!("Occupied slot detected");
            }

            count += 1;
            index = slot.next.load(Relaxed);
        }

        count
    }

    #[test]
    fn test_matching_unit_acquire_drop() {
        let mu = MatchingUnit::<u8>::new(4);
        assert_eq!(self::debug_integrity(&mu), 4);

        let publisher = FramePublisher::null();

        let slot_ref = mu.acquire(publisher).expect("Must get free slot");

        assert_eq!(self::debug_integrity(&mu), 3);

        drop(slot_ref);

        assert_eq!(self::debug_integrity(&mu), 4);
    }

    #[test]
    fn test_matching_unit_cycles_acquire_free() {
        let mu = Arc::new(MatchingUnit::<u8>::new(1000));
        let mut threads = Vec::with_capacity(8);

        for _ in 0..8 {
            let mu_clone = mu.clone();
            threads.push(thread::spawn(move || {
                for _ in 0..1000 {
                    loop {
                        let publisher = FramePublisher::null();
                        if let Some(_) = mu_clone.acquire(publisher) {
                            break;
                        }
                    }
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(self::debug_integrity(&mu), 1000);
    }
}

#[cfg(test)]
mod tests_client {
    use super::*;

    use tokio::net::TcpStream;

    use crate::report::STDIOReporter;

    #[tokio::test]
    async fn test_client_calls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.unwrap();

            negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            let (r, w) = transport.into_split();

            let mut msg_sender = MessageSender::new(w);
            let mut msg_receiver = MessageReceiver::new(r);

            loop {
                match msg_receiver.receive().await {
                    Ok(_) => {
                        let message = msg_receiver.message();
                        let header = Message::decode_header(message).unwrap();
                        match header.directive {
                            Directive::Call => {
                                let op = Message::decode_op(message).unwrap();
                                match op {
                                    1 => {
                                        let params_data = Message::params_data(message).unwrap();
                                        let params: String =
                                            Message::decode_from_slice(params_data).unwrap();
                                        assert_eq!(params, "call");

                                        msg_sender.return_data(&header.id, &"reply").await.unwrap();
                                    }
                                    2 => {
                                        msg_sender
                                            .return_error(
                                                &header.id,
                                                RpcError::error(ErrKind::Unimplemented),
                                            )
                                            .await
                                            .unwrap();
                                    }
                                    _ => panic!("undefined op"),
                                }
                            }
                            Directive::NullaryCall => {
                                let op = Message::decode_op(msg_receiver.message()).unwrap();
                                assert_eq!(op, 1);

                                msg_sender
                                    .return_data(&header.id, &"nullary call reply")
                                    .await
                                    .unwrap();
                            }
                            _ => panic!("Expected call"),
                        }
                    }
                    Err(e) => {
                        println!("Server error: {e}");
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcAsyncClient::connect(1, transport, STDIOReporter::new(), ())
            .await
            .unwrap();

        let reply: String = client.call(1, &"call").await.unwrap();
        assert_eq!(reply, "reply");

        let reply_nullary: String = client.call_nullary(1).await.unwrap();
        assert_eq!(reply_nullary, "nullary call reply");

        let err: RpcError = client.call::<&str, String>(2, &"call").await.unwrap_err();
        assert!(err.kind == ErrKind::Unimplemented);

        client.terminate().await.unwrap();
        server_task.await.unwrap()
    }

    #[tokio::test]
    async fn test_client_encrypted_call_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.unwrap();

            let proposed = negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            assert!(proposed.encryption);

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            assert!(proposed.encryption);

            let (r_key, w_key) = negotiation::accept_key_exchange(&mut transport)
                .await
                .expect("Server encryption setup failed");

            let (r, w) = transport.into_split();

            let mut msg_sender = EncMessageSender::new(w, w_key);
            let mut msg_receiver = EncMessageReceiver::new(r, r_key);

            match msg_receiver.receive().await {
                Ok(_) => {
                    let message = msg_receiver.message();
                    let header = Message::decode_header(message).unwrap();
                    match header.directive {
                        Directive::Call => {
                            let op = Message::decode_op(message).unwrap();
                            assert_eq!(op, 1);

                            let params_data = Message::params_data(message).unwrap();

                            let params: String = Message::decode_from_slice(params_data).unwrap();
                            assert_eq!(params, "call");

                            msg_sender.return_data(&header.id, &"reply").await.unwrap();
                        }
                        _ => panic!("Expected call message"),
                    }
                }
                Err(e) => {
                    println!("Server error: {e}");
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcAsyncClient::connect_encrypted(1, transport, STDIOReporter::new(), ())
            .await
            .unwrap();

        let reply = client.call::<&str, String>(1, &"call").await.unwrap();
        assert_eq!(reply, "reply");

        server_task.await.unwrap();
        client.terminate().await.unwrap();
    }
}
