use std::collections::HashMap;

// Layout:
// [4 bytes BE: subject_len][4 bytes BE: headers_len]
// [subject UTF-8][headers binary][payload bytes]
//
// headers binary:
// [4 bytes BE: count] repeated [4 bytes BE: key_len][4 bytes BE: value_len][key][value]
// The codec encodes only the user payload; this framing only carries routing
// metadata for transports that do not route by subject natively.

pub(crate) struct DecodedFrame<'a> {
    pub subject: &'a str,
    pub headers: Option<HashMap<String, String>>,
    pub payload: &'a [u8],
}

pub(crate) fn encode(
    subject: &str,
    headers: Option<&HashMap<String, String>>,
    payload: &[u8],
) -> Vec<u8> {
    let subject_bytes = subject.as_bytes();
    let headers = encode_headers(headers);
    let mut buf = Vec::with_capacity(8 + subject_bytes.len() + headers.len() + payload.len());
    buf.extend_from_slice(&(subject_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    buf.extend_from_slice(subject_bytes);
    buf.extend_from_slice(&headers);
    buf.extend_from_slice(payload);
    buf
}

pub(crate) fn decode(buf: &[u8]) -> Option<DecodedFrame<'_>> {
    if buf.len() < 8 {
        return None;
    }

    let subject_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let headers_len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    let subject_start = 8;
    let headers_start = subject_start + subject_len;
    let payload_start = headers_start + headers_len;
    if buf.len() < payload_start {
        return None;
    }

    let subject = std::str::from_utf8(&buf[subject_start..headers_start]).ok()?;
    let headers = decode_headers(&buf[headers_start..payload_start])?;
    let payload = &buf[payload_start..];
    Some(DecodedFrame {
        subject,
        headers,
        payload,
    })
}

fn encode_headers(headers: Option<&HashMap<String, String>>) -> Vec<u8> {
    let Some(headers) = headers else {
        return 0u32.to_be_bytes().to_vec();
    };

    let mut buf = Vec::new();
    buf.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    for (key, value) in headers {
        let key = key.as_bytes();
        let value = value.as_bytes();
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
    }
    buf
}

fn decode_headers(buf: &[u8]) -> Option<Option<HashMap<String, String>>> {
    if buf.len() < 4 {
        return None;
    }

    let count = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if count == 0 {
        return Some(None);
    }

    let mut cursor = 4;
    let mut headers = HashMap::with_capacity(count);
    for _ in 0..count {
        if buf.len() < cursor + 8 {
            return None;
        }
        let key_len = u32::from_be_bytes([
            buf[cursor],
            buf[cursor + 1],
            buf[cursor + 2],
            buf[cursor + 3],
        ]) as usize;
        let value_len = u32::from_be_bytes([
            buf[cursor + 4],
            buf[cursor + 5],
            buf[cursor + 6],
            buf[cursor + 7],
        ]) as usize;
        cursor += 8;

        let key_end = cursor + key_len;
        let value_end = key_end + value_len;
        if buf.len() < value_end {
            return None;
        }

        let key = std::str::from_utf8(&buf[cursor..key_end]).ok()?.to_string();
        let value = std::str::from_utf8(&buf[key_end..value_end])
            .ok()?
            .to_string();
        headers.insert(key, value);
        cursor = value_end;
    }

    if cursor != buf.len() {
        return None;
    }

    Some(Some(headers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_round_trips_subject_headers_and_payload() {
        let headers = HashMap::from([
            ("correlation_id".to_string(), "request-1".to_string()),
            (
                "reply_to".to_string(),
                "_qanat.reply.instance-1".to_string(),
            ),
        ]);

        let encoded = encode("orders.created", Some(&headers), b"payload");
        let decoded = decode(&encoded).unwrap();

        assert_eq!(decoded.subject, "orders.created");
        assert_eq!(decoded.headers, Some(headers));
        assert_eq!(decoded.payload, b"payload");
    }

    #[test]
    fn wire_round_trips_without_headers() {
        let encoded = encode("orders.created", None, b"payload");
        let decoded = decode(&encoded).unwrap();

        assert_eq!(decoded.subject, "orders.created");
        assert_eq!(decoded.headers, None);
        assert_eq!(decoded.payload, b"payload");
    }
}
