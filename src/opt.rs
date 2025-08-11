/// Compiler hints to prioritize branches over others and improve branch prediction.
pub(crate) mod branch_prediction {
    #[cold]
    const fn cold_path() {}

    /// Hints to the compiler that branch `condition` is likely to be true.
    /// Returns the value passed to it.
    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) const fn likely(condition: bool) -> bool {
        if condition {
            true
        } else {
            cold_path();
            false
        }
    }

    /// Hints to the compiler that branch `condition` is likely to be false.
    /// Returns the value passed to it.
    #[inline(always)]
    pub(crate) const fn unlikely(condition: bool) -> bool {
        if condition {
            cold_path();
            true
        } else {
            false
        }
    }
}
