// Layout: [4 bytes BE: subject_len][subject UTF-8][payload bytes]
// The codec encodes only the user payload; this framing only carries routing
// metadata for transports that do not route by subject natively.

pub(crate) fn encode(subject: &str, payload: &[u8]) -> Vec<u8> {
    let subject_bytes = subject.as_bytes();
    let mut buf = Vec::with_capacity(4 + subject_bytes.len() + payload.len());
    buf.extend_from_slice(&(subject_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(subject_bytes);
    buf.extend_from_slice(payload);
    buf
}

pub(crate) fn decode(buf: &[u8]) -> Option<(&str, &[u8])> {
    if buf.len() < 4 {
        return None;
    }

    let subject_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + subject_len {
        return None;
    }

    let subject = std::str::from_utf8(&buf[4..4 + subject_len]).ok()?;
    let payload = &buf[4 + subject_len..];
    Some((subject, payload))
}
