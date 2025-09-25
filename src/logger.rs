#[derive(Debug, Clone)]
pub struct ProgressLogger {
    message: String,
    total_count: usize,
}

impl ProgressLogger {
    pub fn new(message: &str, total_count: usize) -> Self {
        Self {
            message: message.to_string(),
            total_count,
        }
    }

    pub fn log_progress(&self, current_count: usize) {
        let percentage = (current_count as f64 / self.total_count as f64) * 100.0;
        print!(
            "\r{}: {}/{} ({:.2}%)",
            self.message, current_count, self.total_count, percentage
        );
    }
}
