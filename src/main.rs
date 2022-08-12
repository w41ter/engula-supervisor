#![feature(backtrace)]

mod base;
mod gen;
mod reader;
mod value;
mod writer;

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use base::Config;
use clap::Parser;
use engula_client::{EngulaClient, Partition};
use rand::{rngs::OsRng, RngCore};
use reader::Reader;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use writer::Writer;

use crate::base::{ExecCtx, Task};

#[derive(Parser)]
struct Args {
    #[clap(required = true, short = 'c', long = "config", parse(from_os_str))]
    config: PathBuf,

    #[clap(short = 'd', long = "dump")]
    dump: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppConfig {
    writers: usize,
    readers: usize,
    hash_slots: u32,

    addrs: Vec<String>,

    db: String,
    collection: String,

    base_seed: Option<u64>,
    generator: Config,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    install_panic_hook();

    let args = Args::parse();
    if args.dump.unwrap_or_default() {
        let cfg = AppConfig::default();
        std::fs::write(&args.config, toml::to_string_pretty(&cfg)?)?;
        info!("dump default config to {} success", args.config.display());
        return Ok(());
    }

    let content = std::fs::read_to_string(&args.config)?;
    let cfg: AppConfig = toml::from_str(&content)?;

    let client = EngulaClient::connect(cfg.addrs).await?;
    info!("connect to engula cluster success");
    let db = client.create_database(cfg.db.clone()).await?;
    info!("create database success");
    let collection = db
        .create_collection(cfg.collection.clone(), Some(Partition::Hash { slots: 5 }))
        .await?;
    info!("create collection success");

    let base_seed = if let Some(base_seed) = cfg.base_seed {
        base_seed
    } else {
        OsRng.next_u64()
    };

    info!("chaos start with base seed {}", base_seed);

    let exec_ctx = ExecCtx::new();

    let mut writers: Vec<Arc<dyn crate::base::Writer>> = vec![];
    let mut writer_handles = vec![];
    for idx in 0..cfg.writers {
        let seed = base_seed.wrapping_add(idx as u64);
        let writer = Arc::new(Writer::new(
            idx,
            seed,
            cfg.generator.clone(),
            collection.clone(),
        ));
        writers.push(writer.clone());
        let cloned_ctx = exec_ctx.clone();
        let handle = tokio::spawn(async move {
            writer.run(cloned_ctx).await;
        });
        writer_handles.push(handle);
    }

    let mut readers: Vec<Arc<dyn crate::base::Reader>> = vec![];
    let mut reader_handles = vec![];
    for idx in 0..cfg.readers {
        if idx >= cfg.writers {
            break;
        }
        let mut traced_writers = vec![];
        let mut writer_idx = idx;
        while writer_idx < cfg.writers {
            traced_writers.push(writers[writer_idx].clone());
            writer_idx += cfg.readers;
        }

        let reader = Arc::new(Reader::new(idx, traced_writers, collection.clone()));
        readers.push(reader.clone());
        let cloned_ctx = ExecCtx::new();
        let handle = tokio::spawn(async move {
            reader.run(cloned_ctx).await;
        });
        reader_handles.push(handle);
    }

    info!("chaos is running");

    for writer in writer_handles {
        writer.await.unwrap_or_default();
    }

    for reader in reader_handles {
        reader.await.unwrap_or_default();
    }

    Ok(())
}

fn install_panic_hook() {
    use std::{panic, process};
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        error!("{:#?}", panic_info);
        error!("{:#?}", std::backtrace::Backtrace::force_capture());
        process::exit(1);
    }));
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            writers: 1,
            readers: 1,
            hash_slots: 255,
            addrs: vec!["127.0.0.1:21805".to_owned()],
            db: "chaos-db".to_owned(),
            collection: "collection".to_owned(),
            base_seed: None,
            generator: Config {
                key_range: 16..32,
                value_range: 512..2048,
            },
        }
    }
}
