use std::{sync::Arc, time::Duration};

use anyhow::Result;
use engula_client::Collection;
use tokio::sync::Mutex;
use tracing::info;

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
    expect_status: TrackerExpectStatus,
    gen: Generator,
    writer: Arc<dyn Writer>,
}

#[allow(unused)]
#[derive(Debug)]
enum TrackerExpectStatus {
    None,
    Existed {
        key: Vec<u8>,
        value: Vec<u8>,
        step: usize,
    },
    Deleted {
        key: Vec<u8>,
    },
}

impl Reader {
    pub fn new(index: usize, writers: Vec<Arc<dyn Writer>>, collection: Collection) -> Self {
        let trackers = writers
            .into_iter()
            .map(|w| WriterTracker {
                accessed_step: 0,
                expect_status: TrackerExpectStatus::None,
                gen: Generator::new(w.seed(), w.config()),
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

    async fn verify_next_op(&mut self, tracker: usize, next_op: &NextOp) -> Result<()> {
        let tracker = &mut self.trackers[tracker];
        match next_op {
            NextOp::Delete { key } => {
                if matches!(tracker.expect_status, TrackerExpectStatus::Deleted { .. }) {
                    tracker.expect_status = TrackerExpectStatus::None;
                }

                if let Some(value) = self.collection.get(key.clone()).await? {
                    let v = Value::from(value.as_slice());
                    let value = v.value();
                    if v.writer() == tracker.writer.index() {
                        if v.index() + 1 < tracker.accessed_step {
                            panic!(
                                "reader {} read a staled key {} writted by writer {}, values is {}",
                                self.index,
                                String::from_utf8_lossy(value.as_slice()),
                                tracker.writer.index(),
                                String::from_utf8_lossy(value.as_slice()),
                            );
                        } else {
                            // This writer will put a value
                            tracker.expect_status = TrackerExpectStatus::Existed {
                                key: key.clone(),
                                value: value.clone(),
                                step: v.index(),
                            };
                        }
                    }
                }
            }
            NextOp::Put { key, value } => {
                if let TrackerExpectStatus::Existed {
                    key: expect_key,
                    step,
                    ..
                } = &tracker.expect_status
                {
                    if *step == tracker.accessed_step && key == expect_key {
                        tracker.expect_status = TrackerExpectStatus::None;
                    }
                }

                match self.collection.get(key.clone()).await? {
                    Some(got_value) => {
                        let v = Value::from(got_value.as_slice());
                        let got_value = v.value();
                        if v.writer() == tracker.writer.index() {
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
                                tracker.expect_status = TrackerExpectStatus::Existed {
                                    key: key.clone(),
                                    value: got_value,
                                    step: v.index(),
                                };
                            }
                        }
                    }
                    None => {}
                };
            }
        }
        Ok(())
    }

    fn verify_and_reset_tracker(&mut self, tracker_index: usize) {
        let tracker = &mut self.trackers[tracker_index];
        match &tracker.expect_status {
            TrackerExpectStatus::Deleted { key } => {
                panic!(
                    "reader {} read key {} should has been deleted by writer {}, access step {}",
                    self.index,
                    String::from_utf8_lossy(key),
                    tracker.writer.index(),
                    tracker.accessed_step,
                );
            }
            TrackerExpectStatus::Existed { key, step, .. } => {
                panic!(
                    "reader {} read key {} should has been written by writer {} at step {}, access step {}",
                    self.index,
                    String::from_utf8_lossy(key),
                    tracker.writer.index(),
                    step,
                    tracker.accessed_step,
                );
            }
            TrackerExpectStatus::None => {
                // passed
                tracker.reset();
            }
        }
    }
}

impl WriterTracker {
    fn reset(&mut self) {
        self.accessed_step = 0;
        self.gen.reset();
        self.expect_status = TrackerExpectStatus::None;
    }
}

#[super::async_trait]
impl super::base::Task for Reader {
    async fn run(&self, mut ctx: ExecCtx) {
        let mut core = self.core.lock().await;
        while let Some(_) = ctx
            .wait_until_timeout_or_shutdown(Duration::from_millis(10))
            .await
        {
            for tracker in 0..core.trackers.len() {
                core.verify(tracker).await;
            }
        }
    }
}

#[super::async_trait]
impl super::base::Reader for Reader {}
