pub struct Value {
    writer: usize,
    index: usize,
    inner: Vec<u8>,
}

impl Value {
    pub fn new(writer: usize, index: usize, inner: Vec<u8>) -> Self {
        Value {
            writer,
            index,
            inner,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let cap = 2 * core::mem::size_of::<usize>() + self.inner.len();
        let mut buf = Vec::with_capacity(cap);
        buf.extend_from_slice(&self.writer.to_le_bytes());
        buf.extend_from_slice(&self.index.to_le_bytes());
        buf.extend_from_slice(&self.inner);
        buf
    }

    #[inline]
    pub fn writer(&self) -> usize {
        self.writer
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    #[inline]
    pub fn value(&self) -> Vec<u8> {
        self.inner.clone()
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        let head = 2 * core::mem::size_of::<usize>();
        if value.len() <= head {
            panic!("value len {} is too small", value.len());
        }

        let mut buf = [0u8; core::mem::size_of::<usize>()];
        buf.as_mut_slice()
            .copy_from_slice(&value[..core::mem::size_of::<usize>()]);
        let writer = usize::from_le_bytes(buf);
        buf.as_mut_slice()
            .copy_from_slice(&value[core::mem::size_of::<usize>()..head]);
        let index = usize::from_le_bytes(buf);
        Value {
            writer,
            index,
            inner: value[head..].to_owned(),
        }
    }
}
