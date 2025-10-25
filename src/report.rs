use std::cell::RefCell;
use std::fmt::{self, Display};
use std::io::Write;

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
/// This trait enables using unified reporters (events' dispatchers/subscribers)
/// without committing to a particular implementation.
///
/// Types implementing this trait can operate on per-instance state,
/// or on shared globals.
///
/// Each implementation can define its own way of formatting and publishing reports,
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
    fn trace<M: ReportMaterial>(&self, description: &str, material: &M);
    fn info<M: ReportMaterial>(&self, description: &str, material: &M);
    fn alert<M: ReportMaterial>(&self, description: &str, material: &M);
    fn error<M: ReportMaterial>(&self, description: &str, material: &M);
}

impl Reporter for () {
    fn trace<M: ReportMaterial>(&self, _description: &str, _material: &M) {}
    fn info<M: ReportMaterial>(&self, _description: &str, _material: &M) {}
    fn alert<M: ReportMaterial>(&self, _description: &str, _material: &M) {}
    fn error<M: ReportMaterial>(&self, _description: &str, _material: &M) {}
}

thread_local! {
    /// Thread local buffer used by reporting agents.
    /// This buffer shall be used for formatting and encoding,
    /// to minimize write calls per part/piece on I/O backends/mediums.
    static REPORT_LOCAL_CACHE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// A reporting agent that uses the standard output of the current process as its reporting medium.
pub struct STDIOReporter {
    out: std::io::Stdout,
    err: std::io::Stderr,
}

impl STDIOReporter {
    /// Creates new `STDIOReporter`.
    #[inline]
    pub fn new() -> Self {
        Self {
            out: std::io::stdout(),
            err: std::io::stderr(),
        }
    }
}

impl Reporter for STDIOReporter {
    #[inline]
    fn trace<M: ReportMaterial>(&self, description: &str, material: &M) {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}TRACE: {description}. {material}{}\n",
                colors::BLUE,
                colors::RESET
            ));
            let _ = self.out.lock().write_all(cache);
            cache.clear();
        });
    }

    #[inline]
    fn info<M: ReportMaterial>(&self, description: &str, material: &M) {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}INFO: {description}. {material}{}\n",
                colors::GREEN,
                colors::RESET
            ));
            let _ = self.out.lock().write_all(cache);
            cache.clear();
        });
    }

    #[inline]
    fn alert<M: ReportMaterial>(&self, description: &str, material: &M) {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}ALERT: {description}. {material}{}\n",
                colors::ORANGE,
                colors::RESET
            ));
            let _ = self.out.lock().write_all(cache);
            cache.clear();
        });
    }

    #[inline]
    fn error<M: ReportMaterial>(&self, description: &str, material: &M) {
        REPORT_LOCAL_CACHE.with_borrow_mut(|cache| {
            let _ = cache.write_fmt(format_args!(
                "{}ERROR: {description}. {material}{}\n",
                colors::BRIGHT_RED,
                colors::RESET
            ));
            let _ = self.err.lock().write_all(cache);
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
    }
}
