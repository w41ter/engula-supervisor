use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use engula_client::Collection;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::{
    base::{ExecCtx, Writer},
    gen::{Generator, NextOp},
    value::Value,
};

pub struct Reader {
    core: Mutex<CoreReader>,
}

struct CoreReader {
    index: usize,
    collection: Collection,
    trackers: Vec<WriterTracker>,
}

struct WriterTracker {
    accessed_step: usize,
    gen: Generator,
    writer: Arc<dyn Writer>,
    expected: HashMap<Vec<u8>, TrackerExpectStatus>,
}

#[allow(unused)]
#[derive(Debug)]
enum TrackerExpectStatus {
    Existed { value: Vec<u8>, step: usize },
    Deleted,
}

impl Reader {
    pub fn new(index: usize, writers: Vec<Arc<dyn Writer>>, collection: Collection) -> Self {
        let trackers = writers
            .into_iter()
            .map(|w| WriterTracker {
                accessed_step: 0,
                gen: Generator::new(w.seed(), w.index() as u64, w.config()),
                expected: HashMap::new(),
                writer: w,
            })
            .collect();
        Reader {
            core: Mutex::new(CoreReader {
                index,
                collection,
                trackers,
            }),
        }
    }
}

impl CoreReader {
    async fn verify(&mut self, tracker_index: usize) {
        let tracker = &mut self.trackers[tracker_index];
        let current_step = tracker.writer.current_step();
        if tracker.accessed_step == current_step {
            info!(
                "reader {} verify one round of writer {}, accessed step {}",
                self.index,
                tracker.writer.index(),
                tracker.accessed_step
            );
            self.verify_and_reset_tracker(tracker_index);
            return;
        }

        debug_assert!(tracker.accessed_step < current_step);
        tracker.accessed_step += 1;
        let next_op = tracker.gen.next_op();
        for _ in 0..120 {
            match self.verify_next_op(tracker_index, &next_op).await {
                Ok(()) => return,
                Err(e) => {
                    tracing::error!("{}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        panic!("could not verify op after 120 secs");
    }

    fn advance_expect_status(&mut self, tracker: usize, next_op: &NextOp) {
        let tracker = &mut self.trackers[tracker];
        match next_op {
            NextOp::Delete { key } => {
                if let Some(expect_status) = tracker.expected.get(key) {
                    if matches!(expect_status, TrackerExpectStatus::Deleted { .. }) {
                        tracker.expected.remove(key);
                    }
                }
            }
            NextOp::Put { key, .. } => {
                if let Some(status) = tracker.expected.get(key) {
                    if matches!(status, TrackerExpectStatus::Existed { step, .. } if *step == tracker.accessed_step)
                    {
                        tracker.expected.remove(key);
                    }
                }
            }
        }
    }

    async fn verify_next_op(&mut self, tracker: usize, next_op: &NextOp) -> Result<()> {
        self.advance_expect_status(tracker, next_op);

        let tracker = &mut self.trackers[tracker];
        match next_op {
            NextOp::Delete { key } => {
                if let Some(value) = self.collection.get(key.clone()).await? {
                    let v = Value::from(value.as_slice());
                    let value = v.value();
                    if v.index() + 1 < tracker.accessed_step {
                        panic!(
                            "reader {} read a staled key {} writted by writer {}, values is {}",
                            self.index,
                            String::from_utf8_lossy(value.as_slice()),
                            tracker.writer.index(),
                            String::from_utf8_lossy(value.as_slice()),
                        );
                    }

                    // This writer will put a value in the corresponding index.
                    tracker.expected.insert(
                        key.clone(),
                        TrackerExpectStatus::Existed {
                            value,
                            step: v.index(),
                        },
                    );
                }
            }
            NextOp::Put { key, value } => {
                match self.collection.get(key.clone()).await? {
                    Some(got_value) => {
                        let v = Value::from(got_value.as_slice());
                        let got_value = v.value();
                        if v.index() + 1 < tracker.accessed_step {
                            panic!(
                                "reader {} read a staled key {} writted by writer {} step {}, values is {}",
                                self.index,
                                String::from_utf8_lossy(key.as_slice()),
                                tracker.writer.index(),
                                v.index(),
                                String::from_utf8_lossy(value.as_slice()),
                            );
                        } else if v.index() == tracker.accessed_step {
                            if got_value != *value {
                                panic!("reader {} read a key {} writted by writer {} with different value",
                                    self.index,
                                    String::from_utf8_lossy(value.as_slice()),
                                    tracker.writer.index(),
                                );
                            }
                        } else {
                            // This writer will put a value in the corresponding index.
                            tracker.expected.insert(
                                key.clone(),
                                TrackerExpectStatus::Existed {
                                    value: value.clone(),
                                    step: v.index(),
                                },
                            );
                        }
                    }
                    None => {
                        tracker
                            .expected
                            .insert(key.clone(), TrackerExpectStatus::Deleted);
                    }
                };
            }
        }
        Ok(())
    }

    fn verify_and_reset_tracker(&mut self, tracker_index: usize) {
        let tracker = &mut self.trackers[tracker_index];

        for (key, expect_status) in &tracker.expected {
            match expect_status {
                TrackerExpectStatus::Deleted => {
                    error!(
                        "reader {} read key {} should has been deleted by writer {}, access step {}",
                        self.index,
                        String::from_utf8_lossy(key),
                        tracker.writer.index(),
                        tracker.accessed_step,
                    );
                }
                TrackerExpectStatus::Existed { step, .. } => {
                    error!(
                        "reader {} read key {} should has been written by writer {} at step {}, access step {}",
                        self.index,
                        String::from_utf8_lossy(key),
                        tracker.writer.index(),
                        step,
                        tracker.accessed_step,
                    );
                }
            }
        }
        if !tracker.expected.is_empty() {
            panic!(
                "reader {} meets {} unresolved expect status",
                self.index,
                tracker.expected.len()
            );
        }

        tracker.reset();
    }
}

impl WriterTracker {
    fn reset(&mut self) {
        self.accessed_step = 0;
        self.gen.reset();
        self.expected = HashMap::new();
    }
}

#[super::async_trait]
impl super::base::Task for Reader {
    async fn run(&self, mut ctx: ExecCtx) {
        let mut core = self.core.lock().await;
        while ctx
            .wait_until_timeout_or_shutdown(Duration::from_millis(10))
            .await
            .is_some()
        {
            for tracker in 0..core.trackers.len() {
                core.verify(tracker).await;
            }
        }
    }
}

#[super::async_trait]
impl super::base::Reader for Reader {}
