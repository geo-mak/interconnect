//! Next generation memory allocators for sending and receiving.

use crate::next::types::core::TypeU64;

pub const ALLOC_MEM_ALIGN: usize = 8;

/// Slice of eight bytes aligned to an 8-byte boundary.
pub type BasicBlock = TypeU64;
