use aead::{Buffer, Error};

use crate::next::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::next::mem::IOSegment;
use crate::next::opt::branch_hints::unlikely;
use crate::next::transport::stream::specs::EncryptionState;
use crate::next::transport::traits::{BytesReceiver, BytesSender};
use crate::next::types::message::MAX_MESSAGE_SIZE;

struct EncryptionAdapter<'a, T> {
    pub seg: &'a mut T,
}

impl<'a, T> EncryptionAdapter<'a, T>
where
    T: IOSegment,
{
    #[inline(always)]
    const fn new(seg: &'a mut T) -> Self {
        Self { seg }
    }
}

impl<'a, T> AsRef<[u8]> for EncryptionAdapter<'a, T>
where
    T: IOSegment,
{
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.seg.as_slice()
    }
}

impl<'a, T> AsMut<[u8]> for EncryptionAdapter<'a, T>
where
    T: IOSegment,
{
    #[inline(always)]
    fn as_mut(&mut self) -> &mut [u8] {
        self.seg.as_slice_mut()
    }
}

impl<'a, T> Buffer for EncryptionAdapter<'a, T>
where
    T: IOSegment,
{
    #[inline(always)]
    fn extend_from_slice(&mut self, other: &[u8]) -> aead::Result<()> {
        // RT_DYN_ALLOC
        if self.seg.write(other) {
            return Ok(());
        }
        Err(Error)
    }

    #[inline(always)]
    fn truncate(&mut self, len: usize) {
        unsafe { self.seg.set_len(len) };
    }
}

struct DecryptionAdapter<'a, T> {
    seg: &'a mut T,
    len: usize,
}

impl<'a, T> DecryptionAdapter<'a, T>
where
    T: IOSegment,
{
    const fn new(buf: &'a mut T, len: usize) -> Self {
        Self { seg: buf, len }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { self.seg.view(self.len) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { self.seg.view_mut(self.len) }
    }
}

impl<'a, T> AsRef<[u8]> for DecryptionAdapter<'a, T>
where
    T: IOSegment,
{
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a, T> AsMut<[u8]> for DecryptionAdapter<'a, T>
where
    T: IOSegment,
{
    #[inline(always)]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl<'a, T> Buffer for DecryptionAdapter<'a, T>
where
    T: IOSegment,
{
    #[inline(always)]
    fn extend_from_slice(&mut self, _other: &[u8]) -> aead::Result<()> {
        Ok(())
    }

    #[inline(always)]
    fn truncate(&mut self, len: usize) {
        unsafe { self.seg.set_len(len) };
    }
}

pub async fn send<T: BytesSender, S: IOSegment>(
    transport: &mut T,
    source: &S,
) -> ProtocolResult<()> {
    // TODO: use protocol types.
    let len_u32 = source.len() as u32;
    // Note: We don't control the segment's layout, so it has to be two calls.
    transport.send_bytes(&len_u32.to_le_bytes()).await?;
    transport.send_bytes(source.as_slice()).await
}

pub async fn receive<T: BytesReceiver, D: IOSegment>(
    transport: &mut T,
    destination: &mut D,
) -> ProtocolResult<()> {
    // TODO: use protocol types.
    let mut len_bytes = [0u8; 4];
    transport.receive_bytes(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes);

    if unlikely(len > MAX_MESSAGE_SIZE) {
        return Err(ProtocolError::error(ErrKind::RecvSizeLimit));
    }

    let len = len as usize;

    // RT_ASSERT.
    // If segment is fresh its len must be 0.
    // If segment is reused, clearing it shall be part of the releasing or acquiring process.
    assert!(destination.len() == 0);

    // Safety: Capacity must be ensured before segmentation.
    if destination.ensure_capacity(len) {
        debug_assert!(destination.capacity() >= len);
        let view_mut = unsafe { destination.view_mut(len) };
        transport.receive_bytes(view_mut).await?;
        // Safety: `len` bytes are assumed to have been initialized.
        unsafe { destination.set_len(len) };
        return Ok(());
    }

    Err(ProtocolError::error(ErrKind::MemoryAllocation))
}

pub async fn send_encrypted<T: BytesSender, S: IOSegment>(
    transport: &mut T,
    source: &mut S,
    state: &mut EncryptionState,
) -> ProtocolResult<()> {
    let mut adapter_segment = EncryptionAdapter::new(source);
    state.encrypt(&mut adapter_segment, b"")?;
    // TODO: use protocol types.
    let len_u32 = source.len() as u32;
    transport.send_bytes(&len_u32.to_le_bytes()).await?;
    transport.send_bytes(source.as_slice()).await
}

pub async fn receive_encrypted<T: BytesReceiver, D: IOSegment>(
    transport: &mut T,
    destination: &mut D,
    state: &mut EncryptionState,
) -> ProtocolResult<()> {
    // TODO: use protocol types.
    let mut len_bytes = [0u8; 4];
    transport.receive_bytes(&mut len_bytes).await?;
    let len = u32::from_le_bytes(len_bytes);

    if unlikely(len > MAX_MESSAGE_SIZE) {
        return Err(ProtocolError::error(ErrKind::RecvSizeLimit));
    }

    let len = len as usize;

    // RT_ASSERT.
    // If segment is fresh its len must be 0.
    // If segment is reused, clearing it shall be part of the releasing or acquiring process.
    assert!(destination.len() == 0);

    // Safety: Capacity must be ensured before segmentation.
    if destination.ensure_capacity(len) {
        debug_assert!(destination.capacity() >= len);

        let view_mut = unsafe { destination.view_mut(len) };

        // Safety: This call must initialize the provided segment or it must fail and return.
        transport.receive_bytes(view_mut).await?;

        // Safety:
        // - Reading is assumed to be done on initialized bytes at this stage.
        // - len is updated after decryption by calling DecryptionAdapter::truncate.
        let mut adapter_segment = DecryptionAdapter::new(destination, len);
        return state.decrypt(&mut adapter_segment, b"");
    }

    Err(ProtocolError::error(ErrKind::MemoryAllocation))
}
