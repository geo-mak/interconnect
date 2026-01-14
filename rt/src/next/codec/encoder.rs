use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::slice::from_raw_parts;

use crate::next::codec::encode::Encode;
use crate::next::codec::limits::TypeLimits;
use crate::next::codec::reference::TypeRef;
use crate::next::error::ProtocolResult;
use crate::next::types::core::ProtocolType;

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
    pub unsafe fn store_next(&mut self, value: &T) {
        #[cfg(debug_assertions)]
        {
            assert!(self.remaining > 0, "Storing beyond the remained capacity");
            self.remaining -= 1;
        }

        let as_bytes_ptr = (value as *const T).cast::<u8>();
        let bytes_slice = unsafe { from_raw_parts(as_bytes_ptr, size_of::<T>()) };
        self.encoder.store_at(self.offset, bytes_slice);
        self.offset += size_of::<T>();
    }
}

pub trait Encoder {
    /// Returns the number of bytes have been initialized in the encoder.
    fn stored_bytes(&self) -> usize;

    /// Sets bytes to zeros.
    ///
    /// More bytes are set to zeros where padding is required.
    fn memset_zeros(&mut self, len: usize);

    /// Appends the provided bytes to the encoder.
    ///
    /// More bytes are added where padding is required.
    fn store(&mut self, src: &[u8]);

    /// Stores bytes at the provided `offset` in the encoder.
    fn store_at(&mut self, offset: usize, src: &[u8]);

    /// Skips number of bytes as reserved space and returns
    /// a type that stores the value at its original offset.
    ///
    /// The skipped bytes will be **zeroed** before returning.
    fn skip<T>(&mut self, len: usize) -> Skip<'_, Self, T> {
        let current_offset = self.stored_bytes();

        self.memset_zeros(len * size_of::<T>());

        Skip {
            encoder: self,
            offset: current_offset,
            _t: PhantomData,
            #[cfg(debug_assertions)]
            remaining: len,
        }
    }

    /// Encodes a group of iterable elements.
    fn encode_next_group<P, T, I>(&mut self, values: I, limits: P::Limits) -> ProtocolResult<()>
    where
        P: ProtocolType + TypeLimits,
        T: Encode<P, Self>,
        I: ExactSizeIterator<Item = T>,
    {
        let mut outputs = self.skip::<P>(values.len());

        let mut inlined = MaybeUninit::<P>::uninit();

        P::write_zero_padding(&mut inlined);

        for value in values {
            value.encode(outputs.encoder, &mut inlined, limits)?;

            let inline_value = unsafe { TypeRef::new_assume_init(&mut inlined) };

            P::check_limits(inline_value, limits)?;

            unsafe {
                outputs.store_next(inlined.assume_init_ref());
            }
        }

        Ok(())
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
