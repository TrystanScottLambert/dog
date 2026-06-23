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

#![warn(clippy::pedantic)]
use anyhow::{anyhow, bail, Result};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const MAGIC: &[u8; 4] = b"PAR1";
const KEY_VALUE_FIELD_ID: i64 = 5; // FileMetaData.key_value_metadata

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ThriftID {
    Stop = 0,
    BoolTrue = 1,
    BoolFalse = 2,
    I8 = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
}

impl TryFrom<u8> for ThriftID {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            0 => ThriftID::Stop,
            1 => ThriftID::BoolTrue,
            2 => ThriftID::BoolFalse,
            3 => ThriftID::I8,
            4 => ThriftID::I16,
            5 => ThriftID::I32,
            6 => ThriftID::I64,
            7 => ThriftID::Double,
            8 => ThriftID::Binary,
            9 => ThriftID::List,
            10 => ThriftID::Set,
            11 => ThriftID::Map,
            12 => ThriftID::Struct,
            _ => bail!("unsupported thrift type id {value}"),
        })
    }
}

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
    if &tail[4..] != MAGIC {
        bail!(
            "{}: missing PAR1 footer magic (encrypted or non-parquet?)",
            output_path.display()
        );
    }
    let metadata_len = u64::from(u32::from_le_bytes(tail[..4].try_into().unwrap()));
    let footer_start = file_len
        .checked_sub(8 + metadata_len)
        .ok_or_else(|| anyhow!("declared footer length exceeds file size"))?;

    let mut blob = vec![0u8; usize::try_from(metadata_len)?];
    file.seek(SeekFrom::Start(footer_start))?;
    file.read_exact(&mut blob)?;
    let new_blob = upsert_kv(&blob, "maml", maml)?;

    // Overwrite only the tail, starting exactly where the old footer began.
    file.seek(SeekFrom::Start(footer_start))?;
    file.write_all(&new_blob)?;
    file.write_all(&(u32::try_from(new_blob.len())?).to_le_bytes())?;
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
        let (type_id, field_id) = read_field_header(blob, &mut pos, &mut last_id)?;

        if let ThriftID::Stop = type_id {
            // No field 5: insert a fresh one right before STOP.
            let field = encode_kv_field(&[(key.to_string(), Some(value.to_string()))]);
            let mut out = Vec::with_capacity(blob.len() + field.len());
            out.extend_from_slice(&blob[..field_start]); // everything before STOP
            out.extend_from_slice(&field);
            out.extend_from_slice(&blob[field_start..]); // the STOP byte
            return Ok(out);
        }

        if field_id == KEY_VALUE_FIELD_ID {
            if type_id != ThriftID::List {
                bail!("key_value_metadata is not a list (thrift type {type_id:?})");
            }
            let (elem_type, count) = read_collection_header(blob, &mut pos)?;
            if count > 0 && elem_type != ThriftID::Struct {
                bail!("key_value_metadata elements are not structs");
            }
            let mut pairs = Vec::with_capacity(usize::try_from(count)? + 1);
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
    out.push(ThriftID::List as u8); // delta 0 (long form) + type LIST
    write_uvarint(&mut out, zigzag(KEY_VALUE_FIELD_ID));
    write_collection_header(&mut out, ThriftID::Struct, pairs.len() as u64);
    for (k, v) in pairs {
        out.push((1 << 4) | ThriftID::Binary as u8); // field 1: key
        write_uvarint(&mut out, k.len() as u64);
        out.extend_from_slice(k.as_bytes());
        if let Some(v) = v {
            out.push((1 << 4) | ThriftID::Binary as u8); // field 2: value
            write_uvarint(&mut out, v.len() as u64);
            out.extend_from_slice(v.as_bytes());
        }
        out.push(ThriftID::Stop as u8);
    }
    out
}

fn parse_key_value(buf: &[u8], pos: &mut usize) -> Result<(String, Option<String>)> {
    let mut last_id = 0i64;
    let mut key: Option<String> = None;
    let mut value: Option<String> = None;
    loop {
        let (type_id, field_id) = read_field_header(buf, pos, &mut last_id)?;
        match type_id {
            ThriftID::Stop => break,
            ThriftID::Binary => {
                let s = read_binary_str(buf, pos)?;
                match field_id {
                    1 => key = Some(s),
                    2 => value = Some(s),
                    _ => {}
                }
            }
            _ => skip_value(buf, pos, type_id)?,
        }
    }
    Ok((
        key.ok_or_else(|| anyhow!("KeyValue missing required key"))?,
        value,
    ))
}

// following are just the keywords that are used in the thrift protocol. Encoding and decoding.
/// Reads the raw array of bytes into the ``type_id``, ``field_id``, and weather it is a stop command.
fn read_field_header(buf: &[u8], pos: &mut usize, last_id: &mut i64) -> Result<(ThriftID, i64)> {
    let buffer = *buf
        .get(*pos)
        .ok_or_else(|| anyhow!("field header out of bounds"))?;
    *pos += 1;
    if buffer == 0 {
        return Ok((ThriftID::Stop, 0));
    }
    let type_id = ThriftID::try_from(buffer & 0x0F)?;
    let delta = i64::from(buffer >> 4);
    let field_id = if delta == 0 {
        unzigzag(read_uvarint(buf, pos)?) // long form
    } else {
        *last_id + delta
    };
    *last_id = field_id;
    Ok((type_id, field_id))
}

// reads the raw array of bytes and converts this into the element type and the number of elements.
// collection referring to list and sets since they are "encoded the same"
// Compact protocol list header (1 byte, short form) and elements:
// +--------+--------+...+--------+
// |sssstttt| elements            |
// +--------+--------+...+--------+
//
// Compact protocol list header (2+ bytes, long form) and elements:
// +--------+--------+...+--------+--------+...+--------+
// |1111tttt| size                | elements            |
// +--------+--------+...+--------+--------+...+--------+
fn read_collection_header(buf: &[u8], pos: &mut usize) -> Result<(ThriftID, u64)> {
    let header = *buf // read the header byte
        .get(*pos)
        .ok_or_else(|| anyhow!("collection header out of bounds"))?;
    *pos += 1; // move on to the next byte
    let elem_type = ThriftID::try_from(header & 0x0F)?; // bit-mask to read the type
    let mut number_of_elements = u64::from(header >> 4); // but mask to read the ssss part (which could be
                                                         // 1111 if it is long form)
    if number_of_elements == 15 {
        // if number_of_elements = 15 it means that we have 1111tttt as the byte.
        // In which case size "size is the size, an unsigned 32-bit varint, 15 or higher (not ZigZag encoded)."
        number_of_elements = read_uvarint(buf, pos)?;
    }
    // else the size was exactly the number_of_elements (4-bit unsigned values 0 - 14)
    Ok((elem_type, number_of_elements))
}

// converts the element type and the count into bytes and adds them to the `out` bytes array.
fn write_collection_header(out: &mut Vec<u8>, elem_type: ThriftID, count: u64) {
    let elem_byte = elem_type as u8;
    if count < 15 {
        // #[allow(clippy::cast_possible_truncation)] // truncation is the point here.
        out.push(
            ((u8::try_from(count).expect("Should not be possible, less than 15")) << 4) | elem_byte,
        );
    } else {
        out.push(0xF0 | elem_byte);
        write_uvarint(out, count);
    }
}

// Updates the pos cursor by skipping over the different types we don't care about.
// See https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
fn skip_value(buf: &[u8], pos: &mut usize, type_id: ThriftID) -> Result<()> {
    match type_id {
        ThriftID::BoolTrue | ThriftID::BoolFalse => {}
        // "values of i8 are encoded as one byte..."
        ThriftID::I8 => *pos += 1,
        // Values of type i16, i32, and i64 are first encoded zigzgag then varint encoded
        ThriftID::I16 | ThriftID::I32 | ThriftID::I64 => {
            read_uvarint(buf, pos)?;
        }
        // Values of type double ... in little-endian byte order (8 bytes)
        ThriftID::Double => *pos += 8,
        // Binary protocol, binary data, 1+ bytes:
        // +--------+...+--------+--------+...+--------+
        // | byte length         | bytes               |
        // +--------+...+--------+--------+...+--------+
        ThriftID::Binary => {
            let len = usize::try_from(read_uvarint(buf, pos)?)?;
            *pos += len;
        }
        ThriftID::List | ThriftID::Set => {
            let (elem_type, count) = read_collection_header(buf, pos)?;
            for _ in 0..count {
                skip_value(buf, pos, elem_type)?;
            }
        }
        // "Maps are encoded with a header indicating the size..."
        // +--------+...+--------+--------+--------+...+--------+
        // | size                |kkkkvvvv| key value pairs     |
        // +--------+...+--------+--------+--------+...+--------+
        // "size if a 32-bit unsigned size, varint encoded (not ZigZag'ed)"
        // size here actually referes to the *number of pairs* that we still need to walk through
        ThriftID::Map => {
            let number_pairs = read_uvarint(buf, pos)?; // this is the number of pairs not size
            if number_pairs > 0 {
                // kkkkvvvv byte telling us type of key and value which we need to skip in turn.
                let kv = *buf
                    .get(*pos)
                    .ok_or_else(|| anyhow!("map header out of bounds"))?;
                *pos += 1; // we've read that byte now
                           // bit-masking to get the key type and value type
                let (kt, vt) = (kv >> 4, kv & 0x0F);
                for _ in 0..number_pairs {
                    skip_value(buf, pos, ThriftID::try_from(kt)?)?;
                    skip_value(buf, pos, ThriftID::try_from(vt)?)?;
                }
            }
        } // See the struct encoding
        ThriftID::Struct => {
            let mut last_id = 0i64; // we are working with deltas so start at zero
            loop {
                let (type_id, _) = read_field_header(buf, pos, &mut last_id)?;
                if type_id == ThriftID::Stop {
                    break;
                }
                skip_value(buf, pos, type_id)?;
            }
        }
        ThriftID::Stop => {
            bail!(
                "Misformed header. STOP found where it shouldn't be and was not handled correctly."
            )
        }
    }
    if *pos > buf.len() {
        bail!("overran footer while parsing");
    }
    Ok(())
}

fn read_binary_str(buf: &[u8], pos: &mut usize) -> Result<String> {
    let len = usize::try_from(read_uvarint(buf, pos)?)?;
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
        result |= (u64::from(byte & 0x7F)) << shift;
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
    ((v << 1) ^ (v >> 63)).cast_unsigned()
}

// convert zigzag encoded unsigned integers to signed integers
fn unzigzag(v: u64) -> i64 {
    ((v >> 1).cast_signed()) ^ -((v & 1).cast_signed())
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

    #[test]
    fn test_bool_skip() {
        let mut pos = 2;
        let buffer = [0u8; 12];
        skip_value(&buffer, &mut pos, ThriftID::BoolTrue).unwrap();
        assert_eq!(pos, 2);
        skip_value(&buffer, &mut pos, ThriftID::BoolFalse).unwrap();
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_i8_skip() {
        let mut pos = 2;
        let buffer: [u8; 5] = [5, 4, 3, 2, 1];
        skip_value(&buffer, &mut pos, ThriftID::I8).unwrap();
        assert_eq!(pos, 3);
        skip_value(&buffer, &mut pos, ThriftID::I8).unwrap();
        assert_eq!(pos, 4);
    }

    #[test]
    fn test_i16_i32_i64_skip() {
        // 300 zigzag-or-not is irrelevant to skipping; as a varint it's [0xAC, 0x02]
        let buf = [0xAC, 0x02, 0xFF];
        let mut pos = 0;
        skip_value(&buf, &mut pos, ThriftID::I64).unwrap();
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_double_skip() {
        let buf = [0u8; 9]; // 8 value bytes + sentinel
        let mut pos = 0;
        skip_value(&buf, &mut pos, ThriftID::Double).unwrap();
        assert_eq!(pos, 8);
    }

    #[test]
    fn test_binary_skip() {
        let mut buffer: Vec<u8> = Vec::new();
        let binary_length = &[5u8];
        let string = b"hello";
        buffer.extend_from_slice(binary_length);
        buffer.extend_from_slice(string);
        let mut pos = 0;
        skip_value(&buffer, &mut pos, ThriftID::Binary).unwrap();
        assert_eq!(pos, 6); // length byte + 5 bytes of hello
    }
    #[test]
    fn test_list() {
        let header = &[(3 << 4) | ThriftID::I8 as u8]; // sssstttt 3 counts of I8
        let items = &[1u8, 2u8, 3u8];
        let mut buffer: Vec<u8> = Vec::new();
        buffer.extend_from_slice(header);
        buffer.extend_from_slice(items);
        let mut pos = 0;
        skip_value(&buffer, &mut pos, ThriftID::List).unwrap();
        assert_eq!(pos, 4);
    }
}
