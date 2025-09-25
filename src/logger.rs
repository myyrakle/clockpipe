#![allow(clippy::print_with_newline)]

use std::io::{self, Write};

#[derive(Debug, Clone)]
pub struct ProgressLogger {
    message: String,
    total_count: usize,
    last_logged_percent: std::cell::Cell<i32>,
}

impl ProgressLogger {
    pub fn new(message: &str, total_count: usize) -> Self {
        Self::new_with_realtime(message, total_count, true)
    }

    pub fn new_with_realtime(message: &str, total_count: usize, _show_realtime: bool) -> Self {
        Self {
            message: message.to_string(),
            total_count,
            last_logged_percent: std::cell::Cell::new(-1),
        }
    }

    pub fn log_progress(&self, current_count: usize) {
        let percentage = (current_count as f64 / self.total_count as f64) * 100.0;
        let percent_int = percentage as i32;

        if percent_int != self.last_logged_percent.get() {
            // 현재 줄을 완전히 지우고 로그 출력
            print!("\r\x1b[K");
            print!(
                "{}: {}/{} ({:.1}%)",
                self.message, current_count, self.total_count, percentage
            );
            io::stdout().flush().unwrap();
            self.last_logged_percent.set(percent_int);
        }

        // 실시간 진행률 표시 (현재 줄 지우고 새로 출력)
        print!(
            "\r\x1b[K{}: {}/{} ({:.1}%)",
            self.message, current_count, self.total_count, percentage
        );
        io::stdout().flush().unwrap();
    }

    pub fn clean(&self) {
        print!("\r\x1b[K");
        print!(
            "{}: {}/{} (100.0%) - Completed\n",
            self.message, self.total_count, self.total_count
        );
        io::stdout().flush().unwrap();
    }
}
