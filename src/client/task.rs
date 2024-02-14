pub struct TaskHandle {
    task: tokio::task::JoinHandle<()>,
}

impl From<tokio::task::JoinHandle<()>> for TaskHandle {
    fn from(task: tokio::task::JoinHandle<()>) -> Self {
        TaskHandle { task }
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}
