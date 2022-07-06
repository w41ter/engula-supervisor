use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

use anyhow::Result;
use engula_client::Collection;
use tracing::debug;

use crate::{
    base::{Config, ExecCtx},
    gen::{Generator, NextOp},
    value::Value,
};

pub struct Writer
where
    Self: Send + Sync,
{
    index: usize,
    step: AtomicUsize,
    collection: Collection,
    core: Mutex<CoreWriter>,
}

struct CoreWriter
where
    Self: Send,
{
    gen: Generator,
}

impl Writer {
    pub fn new(index: usize, seed: u64, config: Config, collection: Collection) -> Self {
        Writer {
            index,
            step: AtomicUsize::new(0),
            collection,
            core: Mutex::new(CoreWriter {
                gen: Generator::new(seed, config),
            }),
        }
    }

    fn next_op(&self) -> NextOp {
        let mut core = self.core.lock().unwrap();
        self.step.fetch_add(1, Ordering::AcqRel);
        core.gen.next_op()
    }

    async fn execute(&self, op: &NextOp) -> Result<()> {
        let step = self.step.load(Ordering::Relaxed);
        match op {
            NextOp::Delete { key } => {
                debug!(
                    "writer {} index {}, delete key {}",
                    self.index,
                    step,
                    String::from_utf8_lossy(key.as_slice()),
                );
                self.collection.delete(key.clone()).await?;
            }
            NextOp::Put { key, value } => {
                debug!(
                    "writer {} index {} put key {} value {}",
                    self.index,
                    step,
                    String::from_utf8_lossy(key.as_slice()),
                    String::from_utf8_lossy(value.as_slice()),
                );
                let v = Value::new(self.index, step, value.clone());
                self.collection.put(key.clone(), v.encode()).await?;
            }
        }
        Ok(())
    }
}

#[super::async_trait]
impl super::base::Task for Writer {
    async fn run(&self, _ctx: ExecCtx) {
        'OUTER: loop {
            let op = self.next_op();
            for _ in 0..120 {
                match self.execute(&op).await {
                    Ok(()) => continue 'OUTER,
                    Err(e) => {
                        tracing::error!("{}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
            panic!("could not execute op after 120 secs");
        }
    }
}

#[super::async_trait]
impl super::base::Writer for Writer {
    fn index(&self) -> usize {
        self.index
    }

    fn current_step(&self) -> usize {
        self.step.load(Ordering::Acquire)
    }

    fn seed(&self) -> u64 {
        let core = self.core.lock().unwrap();
        core.gen.seed()
    }

    fn config(&self) -> Config {
        let core = self.core.lock().unwrap();
        core.gen.config()
    }
}
