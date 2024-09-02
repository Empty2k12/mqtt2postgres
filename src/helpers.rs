use bytes::Bytes;

pub trait IsJson {
    fn is_json(&self) -> bool;
}

impl IsJson for Bytes {
    fn is_json(&self) -> bool {
        return self.first() == Some(&b"{"[0]) && self.last() == Some(&b"}"[0]);
    }
}
