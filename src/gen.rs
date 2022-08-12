use rand::{prelude::SmallRng, Rng, SeedableRng};

use crate::base::Config;

pub enum NextOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

pub struct Generator {
    seed: u64,
    writer: u64,
    cfg: Config,
    rng: SmallRng,
}

impl Generator {
    pub fn new(seed: u64, writer: u64, cfg: Config) -> Self {
        let rng = SmallRng::seed_from_u64(seed);
        Generator {
            seed,
            writer,
            cfg,
            rng,
        }
    }

    #[inline]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    #[inline]
    pub fn config(&self) -> Config {
        self.cfg.clone()
    }

    pub fn reset(&mut self) {
        self.rng = SmallRng::seed_from_u64(self.seed);
    }

    pub fn next_op(&mut self) -> NextOp {
        match self.rng.gen_range(0..2) {
            0 => NextOp::Put {
                key: self.next_key(),
                value: self.next_bytes(self.cfg.value_range.clone()),
            },
            1 => NextOp::Delete {
                key: self.next_key(),
            },
            _ => unreachable!(),
        }
    }

    fn next_key(&mut self) -> Vec<u8> {
        let mut bytes = self.next_bytes(self.cfg.key_range.clone());
        bytes.extend_from_slice(self.writer.to_le_bytes().as_slice());
        bytes
    }

    #[allow(unused)]
    fn writer_from_key(key: &[u8]) -> u64 {
        if key.len() <= 8 {
            panic!("key {key:?} does not contains writer index");
        }

        let len = key.len();
        let mut buf = [0u8; 8];
        buf.as_mut_slice().copy_from_slice(&key[(len - 8)..]);
        u64::from_le_bytes(buf)
    }

    fn next_bytes(&mut self, range: std::ops::Range<usize>) -> Vec<u8> {
        const BYTES: &[u8; 62] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let len = self.rng.gen_range(range);
        let mut buf = vec![0u8; len];
        self.rng.fill(buf.as_mut_slice());
        buf.iter_mut().for_each(|v| *v = BYTES[(*v % 62) as usize]);
        buf
    }
}
