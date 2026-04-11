use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::slice::from_raw_parts;

use crate::codec::encode::Encode;
use crate::codec::reference::TypeRef;
use crate::codec::types::core::ProtocolType;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};

pub struct Skip<'a, E: ?Sized, T> {
    pub encoder: &'a mut E,
    offset: usize,
    _t: PhantomData<T>,

    #[cfg(debug_assertions)]
    remaining: usize,
}

#[cfg(debug_assertions)]
const fn debug_assert_writing_inbounds<E: ?Sized, T>(instance: &mut Skip<'_, E, T>) {
    assert!(
        instance.remaining > 0,
        "Writing beyond the remained capacity"
    );
    instance.remaining -= 1;
}

impl<E, T> Skip<'_, E, T>
where
    E: Encoder + ?Sized,
{
    /// Stores a value to the original offset before skipping.
    ///
    /// Safety: `value` must be fully initialized with added **padding**.
    pub unsafe fn write_next(&mut self, value: &T) {
        debug_assert_writing_inbounds(self);

        let bytes_ptr = (value as *const T).cast::<u8>();

        let value_size = size_of::<T>();

        let value_bytes = unsafe { from_raw_parts(bytes_ptr, value_size) };

        self.encoder.write_encoded_at(self.offset, value_bytes);

        self.offset += value_size;
    }
}

pub trait Encoder {
    /// Returns the number of bytes have been initialized in the encoder.
    fn len_bytes(&self) -> usize;

    /// Sets bytes to zeros.
    ///
    /// More bytes are set to zeros where padding is required.
    ///
    /// Returns `false` in case of lack of memory or failure to allocate more.
    ///
    /// The length of the encoder is advanced to include the zeroed bytes and the padding bytes.
    fn memset_zero(&mut self, count: usize) -> bool;

    /// Appends the provided bytes to the encoder.
    ///
    /// More bytes are added where padding is required.
    ///
    /// Returns `false` in case of lack of memory or failure to allocate more.
    ///
    /// The length of the encoder is advanced to include the encoded bytes and the padding bytes.
    fn write_encoded(&mut self, source: &[u8]) -> bool;

    /// Stores bytes at the provided `offset` in the encoder.
    ///
    /// The length of the encoder remains unchanged.
    fn write_encoded_at(&mut self, offset: usize, source: &[u8]);

    /// Skips number of bytes as reserved space and returns
    /// a type that stores the value at its original offset.
    ///
    /// The skipped bytes will be **zeroed** before returning.
    fn skip<T>(&mut self, count: usize) -> Option<Skip<'_, Self, T>> {
        let current_offset = self.len_bytes();

        let items_bytes = size_of::<T>() * count;

        if !self.memset_zero(items_bytes) {
            return None;
        };

        let instance = Skip {
            encoder: self,
            offset: current_offset,
            _t: PhantomData,

            #[cfg(debug_assertions)]
            remaining: count,
        };

        Some(instance)
    }

    /// Encodes a group of iterable elements into the encoder.
    fn encode_next_group<P, T, I>(&mut self, values: I, limits: P::Limits) -> ProtocolResult<()>
    where
        P: ProtocolType,
        T: Encode<P, Self>,
        I: ExactSizeIterator<Item = T>,
    {
        if let Some(mut outputs) = self.skip::<P>(values.len()) {
            let mut value_store = MaybeUninit::<P>::uninit();

            P::write_zero_padding(&mut value_store);

            for value in values {
                value.encode(outputs.encoder, &mut value_store, limits)?;

                let value_ref = unsafe { TypeRef::new_assume_init(&mut value_store) };

                P::check_limits(value_ref, limits)?;

                unsafe {
                    outputs.write_next(value_store.assume_init_ref());
                }
            }

            return Ok(());
        }

        Err(ProtocolError::error(ErrKind::MemoryAllocation))
    }

    /// Encodes a single value into the encoder.
    fn encode_next<P, T>(&mut self, value: T, limits: P::Limits) -> ProtocolResult<()>
    where
        P: ProtocolType,
        T: Encode<P, Self>,
    {
        self.encode_next_group(core::iter::once(value), limits)
    }
}
