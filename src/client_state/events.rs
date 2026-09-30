//! Best-effort lifecycle hints collected outside authoritative fences.
use std::{
    cell::RefCell,
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::controller::events::{EventBatch, EventSink, NewEvent, PublishAttempt, QueueHint};

const MAX_HINTS: usize = 32;
// Stable, title-free diagnostic: events are best effort and repair handles drops.
static CONTROLLER_EVENT_HINTS_DROPPED: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct PendingHints {
    scopes: usize,
    hints: Vec<(Arc<dyn EventSink>, NewEvent)>,
}

thread_local! { static PENDING: RefCell<PendingHints> = RefCell::default(); }

/// A thread-bound scope. Nested scopes defer release until the outer guard drops.
pub struct DeferredHints {
    sink: Option<Arc<dyn EventSink>>,
    _thread_bound: PhantomData<Rc<()>>,
}

impl DeferredHints {
    pub fn begin(sink: Arc<dyn EventSink>) -> Self {
        Self::enter(Some(sink))
    }

    fn enter(sink: Option<Arc<dyn EventSink>>) -> Self {
        PENDING.with(|pending| pending.borrow_mut().scopes += 1);
        Self {
            sink,
            _thread_bound: PhantomData,
        }
    }

    /// Encloses a log or drain fence even before a nested writer has a sink.
    /// The guard must be declared before the fence, or stored after its FD.
    pub(crate) fn fence() -> Self {
        Self::enter(None)
    }

    pub fn capture(&self, event: NewEvent) {
        if let Some(sink) = &self.sink {
            Self::capture_for(sink, event);
        }
    }

    pub(crate) fn capture_for(sink: &Arc<dyn EventSink>, event: NewEvent) {
        PENDING.with(|pending| {
            let mut pending = pending.borrow_mut();
            if pending.scopes == 0 {
                CONTROLLER_EVENT_HINTS_DROPPED.fetch_add(1, Ordering::Relaxed);
                return;
            }
            if matches!(&event, NewEvent::QueueChanged(_))
                && pending.hints.iter().any(|(binding, event)| {
                    Arc::ptr_eq(binding, sink)
                        && matches!(event, NewEvent::QueueChanged(hint) if hint.turn_id.is_none())
                })
            {
                return;
            }
            let event = if pending.hints.len() == MAX_HINTS
                && matches!(&event, NewEvent::QueueChanged(_))
            {
                pending.hints.retain(|(binding, event)| {
                    !(Arc::ptr_eq(binding, sink) && matches!(event, NewEvent::QueueChanged(_)))
                });
                NewEvent::QueueChanged(QueueHint {
                    turn_id: None,
                    state: None,
                    kind: None,
                    code: None,
                })
            } else {
                event
            };
            if pending.hints.len() == MAX_HINTS {
                CONTROLLER_EVENT_HINTS_DROPPED.fetch_add(1, Ordering::Relaxed);
            } else {
                pending.hints.push((sink.clone(), event));
            }
        });
    }

    pub fn finish(self) {}
}

impl Drop for DeferredHints {
    fn drop(&mut self) {
        let hints = PENDING.with(|pending| {
            let mut pending = pending.borrow_mut();
            pending.scopes -= 1;
            if pending.scopes == 0 {
                std::mem::take(&mut pending.hints)
            } else {
                Vec::new()
            }
        });
        let mut batches: Vec<(Arc<dyn EventSink>, Vec<NewEvent>)> = Vec::new();
        for (sink, event) in hints {
            if let Some((_, batch)) = batches
                .iter_mut()
                .find(|(binding, _)| Arc::ptr_eq(binding, &sink))
            {
                batch.push(event);
            } else {
                batches.push((sink, vec![event]));
            }
        }
        for (sink, events) in batches {
            let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                EventBatch::try_new(events).map(|batch| sink.try_publish(batch))
            }));
            if !matches!(attempt, Ok(Ok(PublishAttempt::Queued))) {
                CONTROLLER_EVENT_HINTS_DROPPED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{cell::RefCell, collections::BTreeSet, sync::Mutex};

    use super::*;
    use crate::controller::events::{EventBatch, PublishAttempt, QueueHint};

    #[derive(Default)]
    pub(crate) struct RecordingSink {
        events: Mutex<Vec<Vec<NewEvent>>>,
    }

    impl RecordingSink {
        pub(crate) fn events(&self) -> Vec<NewEvent> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }
        pub(crate) fn batches(&self) -> Vec<Vec<NewEvent>> {
            self.events.lock().unwrap().clone()
        }
        pub(crate) fn clear(&self) {
            self.events.lock().unwrap().clear();
        }
    }

    thread_local! { static FENCES: RefCell<BTreeSet<&'static str>> = RefCell::default(); }

    struct TestFence(&'static str);
    impl TestFence {
        fn enter(name: &'static str) -> Self {
            FENCES.with(|fences| {
                assert!(fences.borrow_mut().insert(name));
            });
            Self(name)
        }
    }
    impl Drop for TestFence {
        fn drop(&mut self) {
            FENCES.with(|fences| {
                fences.borrow_mut().remove(self.0);
            });
        }
    }

    impl EventSink for RecordingSink {
        fn try_publish(&self, batch: EventBatch) -> PublishAttempt {
            FENCES.with(|fences| assert!(fences.borrow().is_empty(), "released under a fence"));
            self.events.lock().unwrap().push(batch.0);
            PublishAttempt::Queued
        }
    }

    #[test]
    fn durable_hints_are_released_after_all_outer_fences() {
        let sink = Arc::new(RecordingSink::default());
        let scope = DeferredHints::begin(sink.clone());
        {
            let log = TestFence::enter("runner_log");
            let drain = TestFence::enter("drain");
            {
                let state = TestFence::enter("state");
                let nested = DeferredHints::begin(sink.clone());
                nested.capture(NewEvent::ControllerDrainChanged { drained: true });
                assert!(sink.batches().is_empty());
                drop(state);
            }
            assert!(sink.batches().is_empty());
            drop(drain);
            drop(log);
        }
        scope.finish();
        assert_eq!(sink.batches().len(), 1);
        assert_eq!(
            sink.events(),
            vec![NewEvent::ControllerDrainChanged { drained: true }]
        );
        sink.clear();
    }

    #[test]
    fn durable_hints_survive_error_unwinding() {
        let sink = Arc::new(RecordingSink::default());
        let result: Result<(), ()> = (|| {
            let scope = DeferredHints::begin(sink.clone());
            let _state = TestFence::enter("state");
            scope.capture(NewEvent::ControllerDrainChanged { drained: true });
            Err(())
        })();
        assert!(result.is_err());
        assert_eq!(sink.events().len(), 1);
    }

    #[test]
    fn queue_overflow_becomes_one_generic_hint() {
        let sink = Arc::new(RecordingSink::default());
        let scope = DeferredHints::begin(sink.clone());
        for _ in 0..33 {
            scope.capture(NewEvent::QueueChanged(QueueHint {
                turn_id: Some(crate::task::TurnId::generate()),
                state: Some("parked".into()),
                kind: Some("task_turn".into()),
                code: None,
            }));
        }
        scope.finish();
        assert_eq!(
            sink.events(),
            vec![NewEvent::QueueChanged(QueueHint {
                turn_id: None,
                state: None,
                kind: None,
                code: None,
            })]
        );
    }
}
