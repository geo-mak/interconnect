use std::cell::RefCell;
use std::fmt::{self, Display};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::sync::{IORing, ThreadNotify};

pub mod colors {
    pub const RESET: &str = "\x1b[0m";

    // Foreground colors.
    pub const BLACK: &str = "\x1b[30m";
    pub const RED: &str = "\x1b[31m";
    pub const GREEN: &str = "\x1b[32m";
    pub const YELLOW: &str = "\x1b[33m";
    pub const BLUE: &str = "\x1b[34m";
    pub const DARK_BLUE: &str = "\x1b[38;5;17m";
    pub const ORANGE: &str = "\x1b[38;5;208m";
    pub const MAGENTA: &str = "\x1b[35m";
    pub const CYAN: &str = "\x1b[36m";
    pub const WHITE: &str = "\x1b[37m";
    pub const BRIGHT_BLACK: &str = "\x1b[90m";
    pub const BRIGHT_RED: &str = "\x1b[91m";
    pub const BRIGHT_GREEN: &str = "\x1b[92m";
    pub const BRIGHT_YELLOW: &str = "\x1b[93m";
    pub const BRIGHT_BLUE: &str = "\x1b[94m";
    pub const BRIGHT_MAGENTA: &str = "\x1b[95m";
    pub const BRIGHT_CYAN: &str = "\x1b[96m";
    pub const BRIGHT_WHITE: &str = "\x1b[97m";

    // Background colors.
    pub const BG_BLACK: &str = "\x1b[48;5;0m";
    pub const BG_RED: &str = "\x1b[48;5;1m";
    pub const BG_GREEN: &str = "\x1b[48;5;2m";
    pub const BG_YELLOW: &str = "\x1b[48;5;3m";
    pub const BG_BLUE: &str = "\x1b[48;5;4m";
    pub const BG_DARK_BLUE: &str = "\x1b[48;5;17m";
    pub const BG_ORANGE: &str = "\x1b[48;5;208m";
    pub const BG_MAGENTA: &str = "\x1b[48;5;5m";
    pub const BG_CYAN: &str = "\x1b[48;5;6m";
    pub const BG_WHITE: &str = "\x1b[48;5;7m";
    pub const BG_BRIGHT_BLACK: &str = "\x1b[48;5;8m";
    pub const BG_BRIGHT_RED: &str = "\x1b[48;5;9m";
    pub const BG_BRIGHT_GREEN: &str = "\x1b[48;5;10m";
    pub const BG_BRIGHT_YELLOW: &str = "\x1b[48;5;11m";
    pub const BG_BRIGHT_BLUE: &str = "\x1b[48;5;12m";
    pub const BG_BRIGHT_MAGENTA: &str = "\x1b[48;5;13m";
    pub const BG_BRIGHT_CYAN: &str = "\x1b[48;5;14m";
    pub const BG_BRIGHT_WHITE: &str = "\x1b[48;5;15m";
}

/// Types implementing this trait act as detailed context for a report.
///
/// **Currently**, only `Display` is required to be qualified as `ReportMaterial`,
/// but the trait would be extended with methods that enable optimizations.
pub trait ReportMaterial: Display {}

/// A no-op implementation of `ReportMaterial`.
pub struct NoMaterial;

impl Display for NoMaterial {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl<T> ReportMaterial for T where T: Display {}

/// The common unified interface of reporting agents.
///
/// This trait enables using unified reporters (events dispatchers/subscribers)
/// without committing to a particular implementation.
///
/// Types implementing this trait can operate on per-instance state,
/// or on shared globals.
///
/// Each type can defines its own way of formatting and publishing reports,
/// where reporting might be restricted to specific categories and output mediums only.
///
/// Types that generate reports shall disclose details about the reports generated,
/// and when and where they are generated.
///
/// # Parameters
/// - Instance reference (&self): The reference is immutable,
///   because in most cases, the implementation must be `Sync`.
/// - `description`: A short text that describe the event.
/// - `material`: An added detailed context to the event.
///
/// # No-Op
/// - The no-op implementation of reporter is `()`.
/// - The no-op implementation of `ReportMaterial` is `NoMaterial`.
pub trait Reporter {
    fn trace<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial;
    fn info<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial;
    fn alert<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial;
    fn error<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial;
}

impl Reporter for () {
    fn trace<M>(&self, _description: &str, _material: &M)
    where
        M: ReportMaterial,
    {
        ()
    }

    fn info<M>(&self, _description: &str, _material: &M)
    where
        M: ReportMaterial,
    {
        ()
    }

    fn alert<M>(&self, _description: &str, _material: &M)
    where
        M: ReportMaterial,
    {
        ()
    }

    fn error<M>(&self, _description: &str, _material: &M)
    where
        M: ReportMaterial,
    {
        ()
    }
}

thread_local! {
    /// Thread local buffer used by reporting agents.
    /// This buffer shall be used for formatting and encoding,
    /// to minimize write calls per part/piece on I/O backends/mediums.
    static REPORT_LOCAL_CACHE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub const STDIO_BUF_SIZE: usize = 2 * 1024 * 1024;

struct STDIOState {
    io_ring: IORing,
    notify: ThreadNotify,
    canceled: AtomicBool,
}

impl STDIOState {
    #[inline]
    fn new(cap: usize) -> Self {
        Self {
            io_ring: IORing::new(cap),
            notify: ThreadNotify::new(),
            canceled: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    fn is_canceled(&self) -> bool {
        self.canceled.load(std::sync::atomic::Ordering::Acquire)
    }

    #[inline(always)]
    fn set_canceled(&self) {
        self.canceled
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

/// A reporting agent that uses the standard output of the current process as its reporting medium.
///
/// It spawns a dedicated thread for receiving reports' data.
///
/// All clones are by reference, where the underlying state is shared among them all.
#[derive(Clone)]
pub struct STDIOReporter {
    state: Arc<STDIOState>,
}

impl STDIOReporter {
    /// Creates new `STDIOReporter` with default capacity.
    ///
    /// See `STDIO_BUF_SIZE` for more details.
    #[inline]
    pub fn new() -> Self {
        Self::with_capacity(STDIO_BUF_SIZE)
    }

    /// Creates new `STDIOReporter` with custom capacity in **bytes**.
    ///
    /// Reporter is bounded and it will drop reports when it runs out of capacity.
    ///
    /// Capacity must be a power of two and >= 8.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        let state = Arc::new(STDIOState::new(cap));
        Self::run(state.clone());
        Self { state }
    }

    fn run(state: Arc<STDIOState>) {
        std::thread::spawn(move || {
            let mut stdout = std::io::stdout();
            let io_ring = &state.io_ring;
            let notify = &state.notify;
            loop {
                match io_ring.read_published(&mut stdout) {
                    Ok(0) => {
                        // No cancellation checking without locking.
                        let mut lock = state.notify.lock();
                        if !state.is_canceled() {
                           notify.wait(&mut lock)
                        } else {
                            break;
                        }
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
            // Attempt to alert.
            let _ = stdout.write_fmt(format_args!(
                "{}ALERT: STDIOReporter has been terminated{}\n",
                colors::ORANGE,
                colors::RESET,
            ));
        });
    }

    /// Initiates a global shutdown of the reporter instance.
    ///
    /// Shutdown is cooperative, and doesn't interrupt the ongoing writing.
    ///
    /// It will be completed when the current writing ends.
    #[inline]
    pub fn shutdown(&self) {
        // No change to cancellation state without locking.
        let mut _lock = self.state.notify.lock();
        self.state.set_canceled();
        self.state.notify.notify_one();
    }
}

impl Reporter for STDIOReporter {
    #[inline]
    fn trace<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial,
    {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}TRACE: {description}. {material}{}\n",
                colors::BLUE,
                colors::RESET
            ));
            self.state.io_ring.try_write(&cache);
            self.state.notify.notify_one();
            cache.clear();
        });
    }

    #[inline]
    fn info<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial,
    {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}INFO: {description}. {material}{}\n",
                colors::GREEN,
                colors::RESET
            ));
            self.state.io_ring.try_write(&cache);
            self.state.notify.notify_one();
            cache.clear();
        });
    }

    #[inline]
    fn alert<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial,
    {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}ALERT: {description}. {material}{}\n",
                colors::ORANGE,
                colors::RESET
            ));
            self.state.io_ring.try_write(&cache);
            self.state.notify.notify_one();
            cache.clear();
        });
    }

    #[inline]
    fn error<M>(&self, description: &str, material: &M)
    where
        M: ReportMaterial,
    {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}ERROR: {description}. {material}{}\n",
                colors::BRIGHT_RED,
                colors::RESET
            ));
            self.state.io_ring.try_write(&cache);
            self.state.notify.notify_one();
            cache.clear();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stdio_reporter() {
        let instance = STDIOReporter::new();
        instance.trace("Test trace report", &"Test materials");
        instance.info("Test info report", &"Test materials");
        instance.alert("Test alert report", &"Test materials");
        instance.error("Test error report", &"Test materials");
        instance.shutdown();
        assert!(instance.state.is_canceled());
    }
}
