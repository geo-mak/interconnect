use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::slice::from_raw_parts;

use crate::codec::encode::Encode;
use crate::codec::reference::TypeRef;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::types::core::ProtocolType;
use crate::types::limits::TypeLimits;

pub struct Skip<'a, E: ?Sized, T> {
    pub encoder: &'a mut E,
    offset: usize,
    _t: PhantomData<T>,

    #[cfg(debug_assertions)]
    remaining: usize,
}

impl<E, T> Skip<'_, E, T>
where
    E: Encoder + ?Sized,
{
    /// Stores a value to the original offset before skipping.
    ///
    /// Safety: `value` must be fully initialized with added **padding**.
    pub unsafe fn write_next(&mut self, value: &T) {
        #[cfg(debug_assertions)]
        {
            assert!(self.remaining > 0, "Storing beyond the remained capacity");
            self.remaining -= 1;
        }

        let as_bytes_ptr = (value as *const T).cast::<u8>();
        let bytes_slice = unsafe { from_raw_parts(as_bytes_ptr, size_of::<T>()) };
        self.encoder.write_encoded_at(self.offset, bytes_slice);
        self.offset += size_of::<T>();
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
    fn write_zero(&mut self, zeroing_len: usize) -> bool;

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
    fn skip<T>(&mut self, len: usize) -> Option<Skip<'_, Self, T>> {
        let current_offset = self.len_bytes();

        if !self.write_zero(len * size_of::<T>()) {
            return None;
        };

        Some(Skip {
            encoder: self,
            offset: current_offset,
            _t: PhantomData,
            #[cfg(debug_assertions)]
            remaining: len,
        })
    }

    /// Encodes a group of iterable elements.
    fn encode_next_group<P, T, I>(&mut self, values: I, limits: P::Limits) -> ProtocolResult<()>
    where
        P: ProtocolType + TypeLimits,
        T: Encode<P, Self>,
        I: ExactSizeIterator<Item = T>,
    {
        if let Some(mut outputs) = self.skip::<P>(values.len()) {
            let mut inlined = MaybeUninit::<P>::uninit();

            P::write_zero_padding(&mut inlined);

            for value in values {
                value.encode(outputs.encoder, &mut inlined, limits)?;

                let inline_value = unsafe { TypeRef::new_assume_init(&mut inlined) };

                P::check_limits(inline_value, limits)?;

                unsafe {
                    outputs.write_next(inlined.assume_init_ref());
                }
            }

            return Ok(());
        }

        Err(ProtocolError::error(ErrKind::MemoryAllocation))
    }

    /// Encodes a compound value.
    fn encode_next<P, T>(&mut self, value: T, limits: P::Limits) -> ProtocolResult<()>
    where
        P: ProtocolType + TypeLimits,
        T: Encode<P, Self>,
    {
        self.encode_next_group(core::iter::once(value), limits)
    }
}
