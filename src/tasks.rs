use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Debug, Clone)]
pub struct TaskTrackerWithCancellation {
    tracker: TaskTracker,
    token: CancellationToken,
}

impl Default for TaskTrackerWithCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskTrackerWithCancellation {
    pub fn new() -> Self {
        Self {
            tracker: TaskTracker::new(),
            token: CancellationToken::new(),
        }
    }

    pub async fn close(&self) {
        tracing::debug!("TaskTrackerWithCancellation::close");
        tracing::debug!("Canceling tasks");
        self.token.cancel();
        tracing::debug!("Closing tracker");
        self.tracker.close();
        tracing::debug!("Waiting for tasks to complete");
        self.tracker.wait().await;
        tracing::debug!("All tasks completed");
    }
}

static TOKIO_TASK_TRACKER_WITH_CANCELLATION: once_cell::sync::Lazy<TaskTrackerWithCancellation> =
    once_cell::sync::Lazy::new(TaskTrackerWithCancellation::new);

/// Create a new CancellationToken for exit signal
pub fn new_tokio_cancellation_token() -> CancellationToken {
    tracing::debug!("new_tokio_cancellation_token");
    TOKIO_TASK_TRACKER_WITH_CANCELLATION.token.clone()
}

/// Create a new TaskTracker to track task progress
pub fn new_tokio_task_tracker() -> TaskTracker {
    tracing::debug!("new_tokio_task_tracker");
    TOKIO_TASK_TRACKER_WITH_CANCELLATION.tracker.clone()
}

/// Shutdown all tasks, and wait for their completion.
pub async fn cancel_tasks_and_wait_for_completion() {
    TOKIO_TASK_TRACKER_WITH_CANCELLATION.close().await;
}
