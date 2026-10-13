//! Byte-ordered key encodings for the metadata tables.

/// `bucket \0 key` - sorts objects by bucket, then by key bytes, which is
/// exactly S3's listing order.
pub fn object_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(bucket.len() + 1 + key.len());
    k.extend_from_slice(bucket.as_bytes());
    k.push(0);
    k.extend_from_slice(key.as_bytes());
    k
}

/// `[start, end)` covering every object in `bucket`.
pub fn bucket_range(bucket: &str) -> (Vec<u8>, Vec<u8>) {
    let mut start = bucket.as_bytes().to_vec();
    start.push(0);
    let mut end = bucket.as_bytes().to_vec();
    end.push(1);
    (start, end)
}

/// `upload_id \0 part_number(BE)`.
pub fn part_key(upload_id: &str, part: u32) -> Vec<u8> {
    let mut k = upload_id.as_bytes().to_vec();
    k.push(0);
    k.extend_from_slice(&part.to_be_bytes());
    k
}

pub fn upload_parts_range(upload_id: &str) -> (Vec<u8>, Vec<u8>) {
    let mut start = upload_id.as_bytes().to_vec();
    start.push(0);
    let mut end = upload_id.as_bytes().to_vec();
    end.push(1);
    (start, end)
}

/// `bucket \0 key \0 upload_id` - lets uploads be listed per bucket in key order.
pub fn upload_index_key(bucket: &str, key: &str, upload_id: &str) -> Vec<u8> {
    let mut k = object_key(bucket, key);
    k.push(0);
    k.extend_from_slice(upload_id.as_bytes());
    k
}

/// The smallest byte string greater than every string starting with `p`.
pub fn prefix_successor(p: &[u8]) -> Option<Vec<u8>> {
    let mut v = p.to_vec();
    while let Some(last) = v.pop() {
        if last < 0xff {
            v.push(last + 1);
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor() {
        assert_eq!(prefix_successor(b"a/"), Some(b"a0".to_vec()));
        assert_eq!(prefix_successor(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_successor(b"\xff\xff"), None);
    }
}
