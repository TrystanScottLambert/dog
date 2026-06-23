//! In-place editing of the Parquet footer's `key_value_metadata`, inspired by
//! how nanoparquet's `append_parquet` works: read the footer, leave every byte
//! of row-group data untouched, and rewrite only the tail of the file.
//!
//! `write_waves_metadata` upserts a single `maml` key: it drops any existing
//! `maml` entry and appends the new one, preserving all other keys (including
//! `ARROW:schema`). Peak memory is the footer size; no data is decoded or
//! recompressed, and no second copy of the file is made.
//!
//! Limitation: standard, unencrypted footer only (magic `PAR1`).

use anyhow::{anyhow, bail, Result};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const MAGIC: &[u8; 4] = b"PAR1";

// Thrift compact-protocol type ids.
const T_STOP: u8 = 0;
const T_BOOL_TRUE: u8 = 1;
const T_BOOL_FALSE: u8 = 2;
const T_I8: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;

const KEY_VALUE_FIELD_ID: i64 = 5; // FileMetaData.key_value_metadata

/// Insert/replace the `maml` key in `output_path`'s footer, in place.
pub fn write_waves_metadata(output_path: &PathBuf, maml: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(output_path)?;
    let file_len = file.metadata()?.len();
    if file_len < 12 {
        bail!(
            "{}: not a parquet file (smaller than 12 bytes)",
            output_path.display()
        );
    }

    // Last 8 bytes: [metadata_len u32 LE][PAR1].
    let mut tail = [0u8; 8];
    file.seek(SeekFrom::End(-8))?;
    file.read_exact(&mut tail)?;
    if tail[4..] != MAGIC[..] {
        bail!(
            "{}: missing PAR1 footer magic (encrypted or non-parquet?)",
            output_path.display()
        );
    }
    let metadata_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
    let footer_start = file_len
        .checked_sub(8 + metadata_len)
        .ok_or_else(|| anyhow!("declared footer length exceeds file size"))?;

    // Read only the FileMetaData blob (a few KB), then build the new one.
    let mut blob = vec![0u8; metadata_len as usize];
    file.seek(SeekFrom::Start(footer_start))?;
    file.read_exact(&mut blob)?;
    let new_blob = upsert_kv(&blob, "maml", maml)?;

    // Overwrite only the tail, starting exactly where the old footer began.
    file.seek(SeekFrom::Start(footer_start))?;
    file.write_all(&new_blob)?;
    file.write_all(&(new_blob.len() as u32).to_le_bytes())?;
    file.write_all(MAGIC)?;

    // The new footer may be shorter or longer than the old one; set exact length.
    file.set_len(footer_start + new_blob.len() as u64 + 8)?;
    file.sync_all()?;
    Ok(())
}

/// Walk the top-level `FileMetaData` fields, upsert `key`=`value` into the
/// key/value metadata list, and return `(new_footer_blob, key_already_existed)`.
fn upsert_kv(blob: &[u8], key: &str, value: &str) -> Result<Vec<u8>> {
    let mut pos = 0usize;
    let mut last_id = 0i64;

    loop {
        let field_start = pos;
        let (type_id, field_id, is_stop) = read_field_header(blob, &mut pos, &mut last_id)?;

        if is_stop {
            // No field 5: insert a fresh one right before STOP.
            let field = encode_kv_field(&[(key.to_string(), Some(value.to_string()))]);
            let mut out = Vec::with_capacity(blob.len() + field.len());
            out.extend_from_slice(&blob[..field_start]); // everything before STOP
            out.extend_from_slice(&field);
            out.extend_from_slice(&blob[field_start..]); // the STOP byte
            return Ok(out);
        }

        if field_id == KEY_VALUE_FIELD_ID {
            if type_id != T_LIST {
                bail!("key_value_metadata is not a list (thrift type {type_id})");
            }
            let (elem_type, count) = read_collection_header(blob, &mut pos)?;
            if count > 0 && elem_type != T_STRUCT {
                bail!("key_value_metadata elements are not structs");
            }
            let mut pairs = Vec::with_capacity(count as usize + 1);
            for _ in 0..count {
                pairs.push(parse_key_value(blob, &mut pos)?);
            }
            let list_end = pos; // first byte after the list = next field header

            pairs.retain(|(k, _)| k != key);
            pairs.push((key.to_string(), Some(value.to_string())));

            let field = encode_kv_field(&pairs);
            let mut out = Vec::with_capacity(blob.len() + field.len());
            out.extend_from_slice(&blob[..field_start]); // fields before field 5
            out.extend_from_slice(&field); // freshly encoded field 5 (long-form header)
            out.extend_from_slice(&blob[list_end..]); // fields after field 5 + STOP
            return Ok(out);
        }

        skip_value(blob, &mut pos, type_id)?;
    }
}

/// Encode a complete `FileMetaData` field 5 from pairs. Uses the long-form
/// field header (absolute id 5), which keeps neighbouring fields' delta
/// encoding valid regardless of where this lands. This means we shouldn't
/// corrupt the delta encoding.
fn encode_kv_field(pairs: &[(String, Option<String>)]) -> Vec<u8> {
    let mut out = Vec::new();
    #[allow(clippy::identity_op)] // This (0<<4) has no effect but we're showing intent here.
    out.push((0 << 4) | T_LIST); // delta 0 (long form) + type LIST
    write_uvarint(&mut out, zigzag(KEY_VALUE_FIELD_ID));
    write_collection_header(&mut out, T_STRUCT, pairs.len() as u64);
    for (k, v) in pairs {
        out.push((1 << 4) | T_BINARY); // field 1: key
        write_uvarint(&mut out, k.len() as u64);
        out.extend_from_slice(k.as_bytes());
        if let Some(v) = v {
            out.push((1 << 4) | T_BINARY); // field 2: value
            write_uvarint(&mut out, v.len() as u64);
            out.extend_from_slice(v.as_bytes());
        }
        out.push(T_STOP);
    }
    out
}

fn parse_key_value(buf: &[u8], pos: &mut usize) -> Result<(String, Option<String>)> {
    let mut last_id = 0i64;
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;
    loop {
        let (type_id, field_id, is_stop) = read_field_header(buf, pos, &mut last_id)?;
        if is_stop {
            break;
        }
        if type_id == T_BINARY {
            let s = read_binary_str(buf, pos)?;
            match field_id {
                1 => key = Some(s),
                2 => value = Some(s),
                _ => {}
            }
        } else {
            skip_value(buf, pos, type_id)?;
        }
    }
    Ok((
        key.ok_or_else(|| anyhow!("KeyValue missing required key"))?,
        value,
    ))
}

// following are just the keywords that are used in the thrift protocol. Encoding and decoding.

/// Reads the raw array of bytes into the type_id, field_id, and weather it is a stop command.
fn read_field_header(buf: &[u8], pos: &mut usize, last_id: &mut i64) -> Result<(u8, i64, bool)> {
    let b = *buf
        .get(*pos)
        .ok_or_else(|| anyhow!("field header out of bounds"))?;
    *pos += 1;
    if b == 0 {
        return Ok((T_STOP, 0, true));
    }
    let type_id = b & 0x0F;
    let delta = (b >> 4) as i64;
    let field_id = if delta == 0 {
        unzigzag(read_uvarint(buf, pos)?) // long form
    } else {
        *last_id + delta
    };
    *last_id = field_id;
    Ok((type_id, field_id, false))
}

// reads the raw array of bytes and converts this into the element type and the number of elements.
fn read_collection_header(buf: &[u8], pos: &mut usize) -> Result<(u8, u64)> {
    let header = *buf
        .get(*pos)
        .ok_or_else(|| anyhow!("collection header out of bounds"))?;
    *pos += 1;
    let elem_type = header & 0x0F;
    let mut count = (header >> 4) as u64;
    if count == 15 {
        count = read_uvarint(buf, pos)?;
    }
    Ok((elem_type, count))
}

// converts the element type and the count into bytes and adds them to the `out` bytes array.
fn write_collection_header(out: &mut Vec<u8>, elem_type: u8, count: u64) {
    if count < 15 {
        out.push(((count as u8) << 4) | elem_type);
    } else {
        out.push(0xF0 | elem_type);
        write_uvarint(out, count);
    }
}

// Updates the pos cursor by skipping over the different types we don't care about.
fn skip_value(buf: &[u8], pos: &mut usize, type_id: u8) -> Result<()> {
    match type_id {
        T_BOOL_TRUE | T_BOOL_FALSE => {}
        T_I8 => *pos += 1,
        T_I16 | T_I32 | T_I64 => {
            read_uvarint(buf, pos)?;
        }
        T_DOUBLE => *pos += 8,
        T_BINARY => {
            let len = read_uvarint(buf, pos)? as usize;
            *pos += len;
        }
        T_LIST | T_SET => {
            let (elem_type, count) = read_collection_header(buf, pos)?;
            for _ in 0..count {
                skip_value(buf, pos, elem_type)?;
            }
        }
        T_MAP => {
            let count = read_uvarint(buf, pos)?;
            if count > 0 {
                let kv = *buf
                    .get(*pos)
                    .ok_or_else(|| anyhow!("map header out of bounds"))?;
                *pos += 1;
                let (kt, vt) = (kv >> 4, kv & 0x0F);
                for _ in 0..count {
                    skip_value(buf, pos, kt)?;
                    skip_value(buf, pos, vt)?;
                }
            }
        }
        T_STRUCT => {
            let mut last_id = 0i64;
            loop {
                let (t, _id, stop) = read_field_header(buf, pos, &mut last_id)?;
                if stop {
                    break;
                }
                skip_value(buf, pos, t)?;
            }
        }
        other => bail!("unknown thrift type id {other}"),
    }
    if *pos > buf.len() {
        bail!("overran footer while parsing");
    }
    Ok(())
}

fn read_binary_str(buf: &[u8], pos: &mut usize) -> Result<String> {
    let len = read_uvarint(buf, pos)? as usize;
    let end = pos
        .checked_add(len)
        .ok_or_else(|| anyhow!("binary length overflow"))?;
    let bytes = buf
        .get(*pos..end)
        .ok_or_else(|| anyhow!("binary out of bounds"))?;
    let s = String::from_utf8(bytes.to_vec())?;
    *pos = end;
    Ok(s)
}

fn read_uvarint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let byte = *buf
            .get(*pos)
            .ok_or_else(|| anyhow!("varint out of bounds"))?;
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint too long");
        }
    }
    Ok(result)
}

fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

// convert signed integers into unsigned integers via zigzag encoding
fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

// convert zigzag encoded unsigned integers to signed integers
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    // tests for zigzag taken from zigzag crate
    // (https://github.com/Adancurusul/zigzag-rs/blob/master/src/lib.rs)
    #[test]
    fn test_zigzag_i64() {
        assert_eq!(zigzag(0), 0u64);
        assert_eq!(zigzag(-1), 1u64);
        assert_eq!(zigzag(1), 2u64);
        assert_eq!(zigzag(-2), 3u64);
        assert_eq!(zigzag(2), 4u64);
    }

    #[test]
    fn decode_zigzag_i64() {
        assert_eq!(unzigzag(0u64), 0);
        assert_eq!(unzigzag(1u64), -1);
        assert_eq!(unzigzag(2u64), 1);
        assert_eq!(unzigzag(3u64), -2);
        assert_eq!(unzigzag(4u64), 2);
    }

    #[test]
    fn test_write_uvarint() {
        // simple 1 = 1 case
        let mut test_value = Vec::new();
        let ans = vec![1u8];
        write_uvarint(&mut test_value, 1u64);
        assert_eq!(test_value, ans);

        // 60_000 worked example case
        let mut test_value = Vec::new();
        let ans = vec![224u8, 212u8, 3u8];
        write_uvarint(&mut test_value, 60_000u64);
        assert_eq!(test_value, ans);
    }

    #[test]
    fn test_read_uvarint() {
        // simple 1 = 1 case
        let mut pos = 1usize;
        let buffer = vec![2u8, 1u8, 224u8, 212u8, 3u8];

        let res_1 = read_uvarint(&buffer, &mut pos).unwrap();
        assert_eq!(res_1, 1u64);
        let res_60k = read_uvarint(&buffer, &mut pos).unwrap();
        assert_eq!(res_60k, 60_000u64);
    }

    #[test]
    fn test_read_binary_string() {
        let mut pos = 0usize;
        let length_1 = &[5u8];
        let length_2 = &[6u8];
        let string_1 = b"hello";
        let string_2 = b"world!";
        let mut buffer = Vec::new();
        buffer.extend_from_slice(length_1);
        buffer.extend_from_slice(string_1);
        buffer.extend_from_slice(length_2);
        buffer.extend_from_slice(string_2);

        let ans = read_binary_str(&buffer, &mut pos);
        assert_eq!(ans.unwrap(), "hello".to_string());
        let ans = read_binary_str(&buffer, &mut pos);
        assert_eq!(ans.unwrap(), "world!".to_string());
    }
}
