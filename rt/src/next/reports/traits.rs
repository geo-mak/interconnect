use core::fmt::{self, Display};

/// Types implementing this trait act as detailed context for a report.
///
/// **Currently**, only `Display` is required to be qualified as `ReportContent`,
/// but the trait would be extended with methods that enable optimizations.
pub trait ReportContent: Display {}

/// A no-op implementation of `ReportContent`.
pub struct NoContent;

impl Display for NoContent {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

impl<T> ReportContent for T where T: Display {}

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
/// Reporting is granting access to a resource that may be limited, therefore, reporting
/// shall be deployed as explicit "capability", issued via explicit and clear interface.
///
/// Undeclared reporting compromises proper system analysis, and uncontrolled excessive
/// reporting compromises system's stability and performance.
///
/// # Parameters
/// - Instance reference (&self): The reference is immutable,
///   because in most cases, the implementation must be `Sync`.
/// - `description`: A short text that describe the event.
/// - `content`: An added detailed context to the event.
///
/// # No-Op
/// - The no-op implementation of reporter is `()`.
/// - The no-op implementation of `ReportContent` is `NoContent`.
pub trait Reporter {
    fn trace<C: ReportContent>(&self, description: &str, content: &C);
    fn info<C: ReportContent>(&self, description: &str, content: &C);
    fn alert<C: ReportContent>(&self, description: &str, content: &C);
    fn error<C: ReportContent>(&self, description: &str, content: &C);
}

impl Reporter for () {
    fn trace<C: ReportContent>(&self, _description: &str, _content: &C) {}
    fn info<C: ReportContent>(&self, _description: &str, _content: &C) {}
    fn alert<C: ReportContent>(&self, _description: &str, _content: &C) {}
    fn error<C: ReportContent>(&self, _description: &str, _content: &C) {}
}
