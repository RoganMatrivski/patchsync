use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread::{self, JoinHandle};

use crate::sync::FromProgress;

pub trait EventHandler<E>: Send + Sync + 'static {
    fn handle(&self, event: E);
}

impl<E, F: Fn(E) + Send + Sync + 'static> EventHandler<E> for F {
    fn handle(&self, event: E) {
        self(event)
    }
}

impl<E, H: EventHandler<E> + ?Sized> EventHandler<E> for Arc<H> {
    fn handle(&self, event: E) {
        (**self).handle(event);
    }
}

#[allow(dead_code)]
pub(crate) enum Msg<E> {
    Event(E),
    EventWithAck(E, tokio::sync::oneshot::Sender<bool>),
    ProgressWake,
    Shutdown,
}

#[allow(dead_code)]
pub(crate) struct EventSink<E> {
    sender: flume::Sender<Msg<E>>,
    pending_bytes: Arc<AtomicU64>,
}

impl<E> Clone for EventSink<E> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            pending_bytes: Arc::clone(&self.pending_bytes),
        }
    }
}

impl<E> EventSink<E> {
    pub(crate) fn emit(&self, e: E) {
        let _ = self.sender.send(e_to_msg(e));
    }

    pub(crate) async fn emit_and_wait(&self, e: E) -> bool {
        let (ack, done) = tokio::sync::oneshot::channel();
        if self.sender.send(Msg::EventWithAck(e, ack)).is_err() {
            return false;
        }
        done.await.unwrap_or(false)
    }

    pub(crate) fn progress(&self, bytes: usize) {
        let prev = self
            .pending_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        if prev == 0 {
            let _ = self.sender.send(Msg::ProgressWake);
        }
    }
}

fn e_to_msg<E>(e: E) -> Msg<E> {
    Msg::Event(e)
}

#[allow(dead_code)]
pub(crate) struct Dispatcher<E> {
    sender: flume::Sender<Msg<E>>,
    pending_bytes: Arc<AtomicU64>,
    thread_handle: Option<JoinHandle<()>>,
}

impl<E: FromProgress + Send + 'static> Dispatcher<E> {
    pub(crate) fn spawn(handler: impl EventHandler<E>) -> Self {
        let (sender, receiver) = flume::unbounded();
        let pending_bytes = Arc::new(AtomicU64::new(0));
        let pending_bytes_clone = Arc::clone(&pending_bytes);

        let thread_handle = thread::Builder::new()
            .name("patchsync-events".into())
            .spawn(move || {
                let flush = |handler: &dyn EventHandler<E>, pending: &AtomicU64| {
                    let n = pending.swap(0, Ordering::Relaxed);
                    if n > 0 {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            handler.handle(E::from_bytes(n as usize));
                        }));
                    }
                };

                while let Ok(msg) = receiver.recv() {
                    match msg {
                        Msg::Event(e) => {
                            flush(&handler, &pending_bytes_clone);
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                handler.handle(e);
                            }));
                        }
                        Msg::EventWithAck(e, ack) => {
                            flush(&handler, &pending_bytes_clone);
                            let handled =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    handler.handle(e);
                                }))
                                .is_ok();
                            if !handled {
                                tracing::error!("Event handler panicked during awaited event");
                            }
                            let _ = ack.send(handled);
                        }
                        Msg::ProgressWake => {
                            flush(&handler, &pending_bytes_clone);
                        }
                        Msg::Shutdown => {
                            flush(&handler, &pending_bytes_clone);
                            break;
                        }
                    }
                }
            })
            .expect("failed to spawn patchsync-events thread");

        Self {
            sender,
            pending_bytes,
            thread_handle: Some(thread_handle),
        }
    }

    pub(crate) fn sink(&self) -> EventSink<E> {
        EventSink {
            sender: self.sender.clone(),
            pending_bytes: Arc::clone(&self.pending_bytes),
        }
    }

    pub(crate) fn shutdown(mut self) {
        let _ = self.sender.send(Msg::Shutdown);
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl<E> Drop for Dispatcher<E> {
    fn drop(&mut self) {
        if self.thread_handle.is_some() {
            let _ = self.sender.send(Msg::Shutdown);
            if let Some(handle) = self.thread_handle.take() {
                let _ = handle.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestEvent {
        Lifecycle(usize),
        Progress { bytes: usize },
    }

    impl FromProgress for TestEvent {
        fn from_bytes(n: usize) -> Self {
            TestEvent::Progress { bytes: n }
        }
    }

    #[test]
    fn test_lifecycle_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = Arc::clone(&events);

        let dispatcher = Dispatcher::spawn(move |e: TestEvent| {
            events_clone.lock().unwrap().push(e);
        });

        let sink = dispatcher.sink();
        sink.emit(TestEvent::Lifecycle(1));
        sink.emit(TestEvent::Lifecycle(2));
        sink.emit(TestEvent::Lifecycle(3));

        dispatcher.shutdown();

        let recorded = events.lock().unwrap();
        assert_eq!(
            *recorded,
            vec![
                TestEvent::Lifecycle(1),
                TestEvent::Lifecycle(2),
                TestEvent::Lifecycle(3),
            ]
        );
    }

    #[test]
    fn test_progress_sum_with_slow_handler() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = Arc::clone(&events);

        let dispatcher = Dispatcher::spawn(move |e: TestEvent| {
            thread::sleep(Duration::from_millis(5));
            events_clone.lock().unwrap().push(e);
        });

        let sink = dispatcher.sink();
        for _ in 0..10 {
            sink.progress(10);
        }

        dispatcher.shutdown();

        let recorded = events.lock().unwrap();
        let total_bytes: usize = recorded
            .iter()
            .map(|e| match e {
                TestEvent::Progress { bytes } => *bytes,
                _ => 0,
            })
            .sum();

        assert_eq!(total_bytes, 100);
    }

    #[test]
    fn test_progress_never_after_later_lifecycle() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = Arc::clone(&events);

        let dispatcher = Dispatcher::spawn(move |e: TestEvent| {
            events_clone.lock().unwrap().push(e);
        });

        let sink = dispatcher.sink();
        sink.progress(50);
        sink.emit(TestEvent::Lifecycle(1));

        dispatcher.shutdown();

        let recorded = events.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        match &recorded[0] {
            TestEvent::Progress { bytes } => assert_eq!(*bytes, 50),
            _ => panic!("expected progress first"),
        }
        assert_eq!(recorded[1], TestEvent::Lifecycle(1));
    }

    #[tokio::test]
    async fn test_emit_and_wait_waits_for_handler() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let dispatcher = Dispatcher::spawn(move |e: TestEvent| {
            if e == TestEvent::Lifecycle(1) {
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
        });

        let sink = dispatcher.sink();
        let waiting =
            tokio::spawn(async move { sink.emit_and_wait(TestEvent::Lifecycle(1)).await });
        tokio::task::yield_now().await;
        started_rx.recv().unwrap();
        assert!(!waiting.is_finished());

        release_tx.send(()).unwrap();
        assert!(waiting.await.unwrap());
        dispatcher.shutdown();
    }

    #[tokio::test]
    async fn test_emit_and_wait_reports_handler_panic() {
        let dispatcher = Dispatcher::spawn(|_: TestEvent| panic!("boom"));
        let sink = dispatcher.sink();

        assert!(!sink.emit_and_wait(TestEvent::Lifecycle(1)).await);
        dispatcher.shutdown();
    }

    #[test]
    fn test_panicking_handler() {
        let dispatcher = Dispatcher::spawn(|e: TestEvent| {
            if let TestEvent::Lifecycle(99) = e {
                panic!("boom");
            }
        });

        let sink = dispatcher.sink();
        sink.emit(TestEvent::Lifecycle(99));
        sink.emit(TestEvent::Lifecycle(100));

        dispatcher.shutdown();
    }
}
