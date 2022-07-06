use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub key_range: std::ops::Range<usize>,
    pub value_range: std::ops::Range<usize>,
}

pub struct ExecCtx {
    shutdown: (broadcast::Sender<()>, broadcast::Receiver<()>),
}

impl ExecCtx {
    pub fn new() -> Self {
        ExecCtx {
            shutdown: broadcast::channel(1),
        }
    }

    /// Wait until timeout or shutdown.
    pub async fn wait_until_timeout_or_shutdown(&mut self, duration: Duration) -> Option<()> {
        tokio::select! {
            _ = self.shutdown.1.recv() => {
                None
            }
            _ = tokio::time::sleep(duration) => {
                Some(())
            }
        }
    }
}

impl Drop for ExecCtx {
    fn drop(&mut self) {
        self.shutdown.0.send(()).unwrap_or_default();
    }
}

impl Clone for ExecCtx {
    fn clone(&self) -> Self {
        let tx = self.shutdown.0.clone();
        let rx = tx.subscribe();
        ExecCtx { shutdown: (tx, rx) }
    }
}

#[super::async_trait]
pub trait Task: Send + Sync {
    async fn run(&self, ctx: ExecCtx);
}

#[super::async_trait]
pub trait Reader: Task {}

#[super::async_trait]
pub trait Writer: Task {
    fn index(&self) -> usize;

    /// Return the current step of writer.
    fn current_step(&self) -> usize;

    /// Return the seed of the generator of the writer.
    fn seed(&self) -> u64;

    /// Return the config of the writer.
    fn config(&self) -> Config;
}
