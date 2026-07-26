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
    let subject_start = 8usize;
    let headers_start = subject_start.checked_add(subject_len)?;
    let payload_start = headers_start.checked_add(headers_len)?;
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
        return (buf.len() == 4).then_some(None);
    }
    if count > (buf.len() - 4) / 8 {
        return None;
    }

    let mut cursor = 4usize;
    let mut headers = HashMap::with_capacity(count);
    for _ in 0..count {
        let lengths_end = cursor.checked_add(8)?;
        if buf.len() < lengths_end {
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
        cursor = lengths_end;

        let key_end = cursor.checked_add(key_len)?;
        let value_end = key_end.checked_add(value_len)?;
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

    #[test]
    fn wire_rejects_truncated_frames() {
        for len in 0..8 {
            assert!(decode(&[0; 8][..len]).is_none());
        }

        let mut frame = Vec::new();
        frame.extend_from_slice(&10u32.to_be_bytes());
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(b"short");
        assert!(decode(&frame).is_none());
    }

    #[test]
    fn wire_rejects_invalid_subject_utf8() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_be_bytes());
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.push(0xff);
        frame.extend_from_slice(&0u32.to_be_bytes());

        assert!(decode(&frame).is_none());
    }

    #[test]
    fn wire_rejects_malformed_header_sections() {
        let mut missing_lengths = Vec::new();
        missing_lengths.extend_from_slice(&1u32.to_be_bytes());
        missing_lengths.extend_from_slice(&4u32.to_be_bytes());
        missing_lengths.extend_from_slice(b"s");
        missing_lengths.extend_from_slice(&1u32.to_be_bytes());
        assert!(decode(&missing_lengths).is_none());

        let mut trailing_header_data = Vec::new();
        trailing_header_data.extend_from_slice(&1u32.to_be_bytes());
        trailing_header_data.extend_from_slice(&5u32.to_be_bytes());
        trailing_header_data.extend_from_slice(b"s");
        trailing_header_data.extend_from_slice(&0u32.to_be_bytes());
        trailing_header_data.push(0);
        assert!(decode(&trailing_header_data).is_none());
    }

    #[test]
    fn wire_rejects_impossible_header_count_before_allocating() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_be_bytes());
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(b"s");
        frame.extend_from_slice(&u32::MAX.to_be_bytes());

        assert!(decode(&frame).is_none());
    }
}
