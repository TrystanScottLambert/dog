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
/// This is the custom waves maml function. But we could upsert to be generalized in the future
pub fn write_keyword_metadata(
    output_path: &PathBuf,
    file_contents: &str,
    keyword: &str,
) -> Result<()> {
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
    let new_blob = upsert_kv(&blob, keyword, file_contents)?;

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
/// This is the general add a new key-word value in the metadata
fn upsert_kv(metadata_blob: &[u8], key: &str, value: &str) -> Result<Vec<u8>> {
    let mut pos = 0usize;
    let mut last_id = 0i64;

    loop {
        let field_start = pos;
        let (type_id, field_id) = read_field_header(metadata_blob, &mut pos, &mut last_id)?;

        if let ThriftID::Stop = type_id {
            // No field 5: insert a fresh one right before STOP.
            let field = encode_kv_field(&[(key.to_string(), Some(value.to_string()))]);
            let mut out = Vec::with_capacity(metadata_blob.len() + field.len());
            out.extend_from_slice(&metadata_blob[..field_start]); // everything before STOP
            out.extend_from_slice(&field);
            out.extend_from_slice(&metadata_blob[field_start..]); // the STOP byte
            return Ok(out);
        }

        if field_id == KEY_VALUE_FIELD_ID {
            if type_id != ThriftID::List {
                bail!("key_value_metadata is not a list (thrift type {type_id:?})");
            }
            let (elem_type, count) = read_collection_header(metadata_blob, &mut pos)?;
            if count > 0 && elem_type != ThriftID::Struct {
                bail!("key_value_metadata elements are not structs");
            }
            let mut pairs = Vec::with_capacity(usize::try_from(count)? + 1);
            for _ in 0..count {
                pairs.push(parse_key_value(metadata_blob, &mut pos)?);
            }
            let list_end = pos; // first byte after the list = next field header

            pairs.retain(|(k, _)| k != key);
            pairs.push((key.to_string(), Some(value.to_string())));

            let field = encode_kv_field(&pairs);
            let mut out = Vec::with_capacity(metadata_blob.len() + field.len());
            out.extend_from_slice(&metadata_blob[..field_start]); // fields before field 5
            out.extend_from_slice(&field); // freshly encoded field 5 (long-form header)
            out.extend_from_slice(&metadata_blob[list_end..]); // fields after field 5 + STOP
            return Ok(out);
        }

        skip_value(metadata_blob, &mut pos, type_id)?;
    }
}

/// Walk the top-level `FileMetaData` fields and rebuilds, ignoring `key`
fn delete_kv(metadata_blob: &[u8], key: &str) -> Result<Vec<u8>> {
    let mut pos = 0usize;
    let mut last_id = 0i64;

    loop {
        let field_start = pos;
        let (type_id, field_id) = read_field_header(metadata_blob, &mut pos, &mut last_id)?;

        if let ThriftID::Stop = type_id {
            bail!("No keyword metadata. Nothing to delete.")
        }

        if field_id == KEY_VALUE_FIELD_ID {
            if type_id != ThriftID::List {
                bail!("key_value_metadata is not a list (thrift type {type_id:?})");
            }
            let (elem_type, count) = read_collection_header(metadata_blob, &mut pos)?;
            if count > 0 && elem_type != ThriftID::Struct {
                bail!("key_value_metadata elements are not structs");
            }
            let mut pairs = Vec::with_capacity(usize::try_from(count)? + 1);
            for _ in 0..count {
                pairs.push(parse_key_value(metadata_blob, &mut pos)?);
            }
            let list_end = pos; // first byte after the list = next field header

            pairs.retain(|(k, _)| k != key);

            let field = encode_kv_field(&pairs);
            let mut out = Vec::with_capacity(metadata_blob.len() + field.len());
            out.extend_from_slice(&metadata_blob[..field_start]); // fields before field 5
            out.extend_from_slice(&field); // freshly encoded field 5 (long-form header)
            out.extend_from_slice(&metadata_blob[list_end..]); // fields after field 5 + STOP
            return Ok(out);
        }

        skip_value(metadata_blob, &mut pos, type_id)?;
    }
}

pub fn delete_keyword_metadata(output_path: &PathBuf, keyword: &str) -> Result<()> {
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
    let new_blob = delete_kv(&blob, keyword)?;

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
// Spec here is in https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
//  * Wrapper struct to store key values
//  */
//  struct KeyValue {
//   1: required string key
//   2: optional string value
// }
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
                    _ => {} // "... it is also possible to handle unknown fields while decoding by ignoring them..."
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
// Reads the raw array of bytes into the ``type_id``, ``field_id``, and weather it is a stop command.
// Field ID here is just counting the entries 1, 2, 3 etc. but using the deltas
// Compact protocol field header (short form) and field value:
// +--------+--------+...+--------+
// |ddddtttt| field value         |
// +--------+--------+...+--------+
//
// Compact protocol field header (1 to 3 bytes, long form) and field value:
// +--------+--------+...+--------+--------+...+--------+
// |0000tttt| field id            | field value         |
// +--------+--------+...+--------+--------+...+--------+
//
// Compact protocol stop-field:
// +--------+
// |00000000|
// +--------+
fn read_field_header(buf: &[u8], pos: &mut usize, last_id: &mut i64) -> Result<(ThriftID, i64)> {
    let buffer = *buf
        .get(*pos)
        .ok_or_else(|| anyhow!("field header out of bounds"))?; // read the first byte
    *pos += 1; // move pointer to next byte
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
// Collection headers are referring to lists and sets.
//
fn write_collection_header(out: &mut Vec<u8>, element_type: ThriftID, count: u64) {
    let elem_byte = element_type as u8;
    if count < 15 {
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
    fn test_write_collection_header_small_count() {
        let mut buffer = Vec::new();
        let element_type = ThriftID::Binary;
        let count = 10;
        write_collection_header(&mut buffer, element_type, count);
        let answer = (u8::try_from(count).unwrap() << 4) | (ThriftID::Binary as u8);
        assert_eq!(buffer, vec![answer]);
    }
    #[test]
    fn test_write_collection_header_big_count() {
        let mut buffer = Vec::new();
        let element_type = ThriftID::Binary;
        let count = 60_000;
        write_collection_header(&mut buffer, element_type, count);
        let answer_header = 0xF0 | (ThriftID::Binary as u8);
        let answer = vec![answer_header, 224u8, 212u8, 3u8];
        assert_eq!(buffer, answer);
    }

    #[test]
    fn test_read_collection_header() {
        // taking the worked example from write collection headers
        let count = 10;
        let mut pos = 0;
        let short_form_header =
            vec![(u8::try_from(count).unwrap() << 4) | (ThriftID::Binary as u8)];
        let long_form_header = vec![0xF0 | (ThriftID::Binary as u8), 224u8, 212u8, 3u8];
        let (answer_id, count) = read_collection_header(&short_form_header, &mut pos).unwrap();
        assert_eq!(answer_id, ThriftID::Binary);
        assert_eq!(count, 10);
        let mut pos = 0;
        let (answer_id, count) = read_collection_header(&long_form_header, &mut pos).unwrap();
        assert_eq!(answer_id, ThriftID::Binary);
        assert_eq!(count, 60_000);
    }

    #[test]
    fn test_read_field_header() {
        let mut last_id = 0;
        let mut pos = 0;
        let string_length = 3u8;
        let string_value = b"dog";
        let short_form_header = (0x01 << 4) | (ThriftID::Binary as u8);
        let mut short_1_buffer = Vec::new();
        // first field 1 is a string called "dog"
        short_1_buffer.extend_from_slice(&[short_form_header]);
        short_1_buffer.extend_from_slice(&[string_length]);
        short_1_buffer.extend_from_slice(string_value);

        // field 2 is a string called "dogs"
        let mut short_2_buffer = Vec::new();
        short_2_buffer.extend_from_slice(&[short_form_header]);
        short_2_buffer.extend_from_slice(&[4]);
        short_2_buffer.extend_from_slice(b"dogs");

        // field 4 is a longform id with a int64 value of 123
        let mut long_buffer = Vec::new();
        let long_form_header = [ThriftID::I64 as u8, u8::try_from(zigzag(4)).unwrap()];
        let mut long_form_value = Vec::new();
        write_uvarint(&mut long_form_value, zigzag(64));
        long_buffer.extend_from_slice(&long_form_header);
        long_buffer.extend_from_slice(&long_form_value);

        // Stop field at the end
        let mut stop_buffer = Vec::new();
        stop_buffer.extend_from_slice(&[0u8]);

        let (res_id, res_64) = read_field_header(&short_1_buffer, &mut pos, &mut last_id).unwrap();
        assert_eq!(res_id, ThriftID::Binary);
        assert_eq!(res_64, 1);
        assert_eq!(pos, 1); // just read the header. The full update is handled with skip

        pos = 0;
        last_id = 1;
        let (res_id, res_64) = read_field_header(&short_2_buffer, &mut pos, &mut last_id).unwrap();
        assert_eq!(res_id, ThriftID::Binary);
        assert_eq!(res_64, 2);
        assert_eq!(pos, 1);

        pos = 0;
        last_id = 5;
        let (res_id, res_64) = read_field_header(&long_buffer, &mut pos, &mut last_id).unwrap();
        assert_eq!(res_id, ThriftID::I64);
        assert_eq!(res_64, 4);

        pos = 0;
        last_id = 5;
        let (res_id, res_64) = read_field_header(&stop_buffer, &mut pos, &mut last_id).unwrap();
        assert_eq!(res_id, ThriftID::Stop);
        assert_eq!(res_64, 0);
    }

    #[cfg(test)]
    mod test_skips {
        use super::*;

        fn fhead(delta: u8, t: ThriftID) -> u8 {
            (delta << 4) | t as u8
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
        fn test_list_i8_skip() {
            let header = &[(3 << 4) | ThriftID::I8 as u8]; // sssstttt 3 counts of I8
            let items = &[1u8, 2u8, 3u8];
            let mut buffer: Vec<u8> = Vec::new();
            buffer.extend_from_slice(header);
            buffer.extend_from_slice(items);
            let mut pos = 0;
            skip_value(&buffer, &mut pos, ThriftID::List).unwrap();
            assert_eq!(pos, 4);
        }
        #[test]
        fn test_empty_map_skip() {
            let buf = [0x00, 0xFF]; // count 0, no kv-type byte follows
            let mut pos = 0;
            skip_value(&buf, &mut pos, ThriftID::Map).unwrap();
            assert_eq!(pos, 1);
        }

        #[test]
        fn test_map_one_i8_to_i8_skip() {
            let kv = ((ThriftID::I8 as u8) << 4) | ThriftID::I8 as u8;
            let buf = [0x01, kv, 0xAA, 0xBB, 0xFF]; // count, kv-types, key, value
            let mut pos = 0;
            skip_value(&buf, &mut pos, ThriftID::Map).unwrap();
            assert_eq!(pos, 4);
        }

        #[test]
        fn test_struct_skip() {
            // field id 1, type I8, value 0x42, then STOP
            let buf = [fhead(1, ThriftID::I8), 0x42, ThriftID::Stop as u8, 0xFF];
            let mut pos = 0;
            skip_value(&buf, &mut pos, ThriftID::Struct).unwrap();
            assert_eq!(pos, 3); // header + value + stop  -- FAILS on the inverted arm
        }

        #[test]
        fn test_empty_struct_skip() {
            let buf = [ThriftID::Stop as u8, 0xFF];
            let mut pos = 0;
            skip_value(&buf, &mut pos, ThriftID::Struct).unwrap();
            assert_eq!(pos, 1); // -- the inverted arm bails here instead
        }

        #[test]
        fn test_nested_list_of_structs_skip() {
            // exercises recursion: list of 2 structs, each = one I8 field then STOP
            let one = [fhead(1, ThriftID::I8), 0x09, ThriftID::Stop as u8];
            let mut buf = vec![(2 << 4) | ThriftID::Struct as u8];
            buf.extend_from_slice(&one);
            buf.extend_from_slice(&one);
            buf.push(0xFF);
            let mut pos = 0;
            skip_value(&buf, &mut pos, ThriftID::List).unwrap();
            assert_eq!(pos, 1 + 3 + 3);
        }

        #[test]
        fn test_stop_errors_skip() {
            let mut pos = 0;
            assert!(skip_value(&[0x00], &mut pos, ThriftID::Stop).is_err());
        }

        #[test]
        fn test_truncated_binary_errors_skips() {
            // claims length 16 but the bytes aren't there
            let mut pos = 0;
            assert!(skip_value(&[0x10], &mut pos, ThriftID::Binary).is_err());
        }
    }

    #[cfg(test)]
    mod test_parse_key {
        use super::*;

        #[test]
        fn test_parse_key_value_simple() {
            let mut key_value = Vec::new();
            let mut pos = 0;
            let field_id = (0x01 << 4) | ThriftID::Binary as u8;
            // field 1 (key)
            key_value.extend_from_slice(&[field_id]);
            key_value.extend_from_slice(&[u8::try_from(3).unwrap()]);
            key_value.extend_from_slice(b"key");

            // field 2 (value)
            key_value.extend_from_slice(&[field_id]);
            key_value.extend_from_slice(&[u8::try_from(5).unwrap()]);
            key_value.extend_from_slice(b"value");

            // stop
            key_value.extend_from_slice(&[0u8]);

            let (res_key, res_value) = parse_key_value(&key_value, &mut pos).unwrap();
            assert_eq!(res_key, "key".to_string());
            assert!(res_value.is_some());
            assert_eq!(res_value.unwrap(), "value".to_string());
        }

        #[test]
        fn test_parse_key_value_missing_value() {
            let mut key_value = Vec::new();
            let mut pos = 0;
            let field_id = (0x01 << 4) | ThriftID::Binary as u8;
            // field 1 (key)
            key_value.extend_from_slice(&[field_id]);
            key_value.extend_from_slice(&[u8::try_from(3).unwrap()]);
            key_value.extend_from_slice(b"key");

            // stop
            key_value.extend_from_slice(&[0u8]);

            let (res_key, res_value) = parse_key_value(&key_value, &mut pos).unwrap();
            assert_eq!(res_key, "key".to_string());
            assert!(res_value.is_none());
        }
        #[test]
        fn test_parse_key_value_extra_field_ignored() {
            let mut key_value = Vec::new();
            let mut pos = 0;
            let field_id = (0x01 << 4) | ThriftID::Binary as u8;

            // field 1 (key)
            key_value.extend_from_slice(&[field_id]);
            key_value.extend_from_slice(&[u8::try_from(3).unwrap()]);
            key_value.extend_from_slice(b"key");

            // field 2 (value)
            key_value.extend_from_slice(&[field_id]);
            key_value.extend_from_slice(&[u8::try_from(5).unwrap()]);
            key_value.extend_from_slice(b"value");

            // field 3
            key_value.extend_from_slice(&[field_id]);
            key_value.extend_from_slice(&[u8::try_from(6).unwrap()]);
            key_value.extend_from_slice(b"ignore");

            // stop
            key_value.extend_from_slice(&[0u8]);

            let (res_key, res_value) = parse_key_value(&key_value, &mut pos).unwrap();
            assert_eq!(res_key, "key".to_string());
            assert!(res_value.is_some());
            assert_eq!(res_value.unwrap(), "value".to_string());
        }
    }

    #[cfg(test)]
    mod test_encode_kv {
        use super::*;
        #[test]
        fn test_encode_kv() {
            let kvs = [
                ("key_1".to_string(), Some("value_1".to_string())),
                ("key_2".to_string(), Some("value_2".to_string())),
                ("key_3".to_string(), None),
            ];
            let struct_header = (1u8 << 4) | ThriftID::Binary as u8;
            let result = encode_kv_field(&kvs);

            let mut answer = Vec::new();
            answer.extend_from_slice(&[ThriftID::List as u8]);
            answer.extend_from_slice(&[u8::try_from(zigzag(5)).unwrap()]); // field id of kv metadata
            answer.extend_from_slice(&[(3 << 4) | ThriftID::Struct as u8]); // elements and type
            answer.extend_from_slice(&[struct_header]);
            answer.extend_from_slice(&[5u8]);
            answer.extend_from_slice(b"key_1");
            answer.extend_from_slice(&[struct_header]);
            answer.extend_from_slice(&[7u8]);
            answer.extend_from_slice(b"value_1");
            answer.extend_from_slice(&[0u8]);

            answer.extend_from_slice(&[struct_header]);
            answer.extend_from_slice(&[5u8]);
            answer.extend_from_slice(b"key_2");
            answer.extend_from_slice(&[struct_header]);
            answer.extend_from_slice(&[7u8]);
            answer.extend_from_slice(b"value_2");
            answer.extend_from_slice(&[0u8]);

            answer.extend_from_slice(&[struct_header]);
            answer.extend_from_slice(&[5u8]);
            answer.extend_from_slice(b"key_3");
            answer.extend_from_slice(&[0u8]);
            assert_eq!(result, answer);
        }
    }

    #[cfg(test)]
    #[allow(clippy::cast_possible_truncation)] // test fixtures use known-small values
    mod upsert_tests {
        use super::*;

        // short-form field header: id delta in high nibble, type in low nibble
        fn fhead(delta: u8, t: ThriftID) -> u8 {
            (delta << 4) | (t as u8)
        }

        // one KeyValue struct: key (field 1) + optional value (field 2) + STOP
        fn kv_struct(key: &str, value: Option<&str>) -> Vec<u8> {
            let mut b = vec![fhead(1, ThriftID::Binary), key.len() as u8];
            b.extend_from_slice(key.as_bytes());
            if let Some(v) = value {
                b.push(fhead(1, ThriftID::Binary));
                b.push(v.len() as u8);
                b.extend_from_slice(v.as_bytes());
            }
            b.push(ThriftID::Stop as u8);
            b
        }

        // a FileMetaData footer whose field 5 holds `pairs`, then the struct STOP.
        // field 5 is written long-form (header + zigzag(5)), like upsert itself emits.
        fn footer_with_kv(pairs: &[(&str, Option<&str>)]) -> Vec<u8> {
            let mut b = vec![ThriftID::List as u8, zigzag(5) as u8];
            b.push(((pairs.len() as u8) << 4) | (ThriftID::Struct as u8)); // count < 15
            for (k, v) in pairs {
                b.extend_from_slice(&kv_struct(k, *v));
            }
            b.push(ThriftID::Stop as u8);
            b
        }

        // same, but with a leading i64 field (id 3) that must be skipped first
        fn footer_with_leading_field(pairs: &[(&str, Option<&str>)]) -> Vec<u8> {
            let mut b = vec![fhead(3, ThriftID::I64)];
            write_uvarint(&mut b, zigzag(42)); // num_rows-ish value
            b.push(ThriftID::List as u8);
            b.push(zigzag(5) as u8);
            b.push(((pairs.len() as u8) << 4) | (ThriftID::Struct as u8));
            for (k, v) in pairs {
                b.extend_from_slice(&kv_struct(k, *v));
            }
            b.push(ThriftID::Stop as u8);
            b
        }

        // read-only twin of upsert_kv: walk to field 5 and decode its pairs
        fn read_kv_pairs(blob: &[u8]) -> Result<Vec<(String, Option<String>)>> {
            let mut pos = 0usize;
            let mut last_id = 0i64;
            loop {
                let (type_id, field_id) = read_field_header(blob, &mut pos, &mut last_id)?;
                if type_id == ThriftID::Stop {
                    return Ok(Vec::new()); // no field 5 present
                }
                if field_id == KEY_VALUE_FIELD_ID {
                    let (_elem, count) = read_collection_header(blob, &mut pos)?;
                    let mut pairs = Vec::new();
                    for _ in 0..count {
                        pairs.push(parse_key_value(blob, &mut pos)?);
                    }
                    return Ok(pairs);
                }
                skip_value(blob, &mut pos, type_id)?;
            }
        }

        // build expected owned pairs for assertions
        fn owned(pairs: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
                .collect()
        }

        #[test]
        fn inserts_field5_when_absent() {
            let blob = [ThriftID::Stop as u8]; // empty FileMetaData struct
            let out = upsert_kv(&blob, "maml", "hello").unwrap();
            assert_eq!(
                read_kv_pairs(&out).unwrap(),
                owned(&[("maml", Some("hello"))])
            );
        }

        #[test]
        fn appends_when_key_absent() {
            let blob = footer_with_kv(&[("a", Some("1"))]);
            let out = upsert_kv(&blob, "maml", "v").unwrap();
            assert_eq!(
                read_kv_pairs(&out).unwrap(),
                owned(&[("a", Some("1")), ("maml", Some("v"))])
            );
        }

        #[test]
        fn replaces_existing_key_and_preserves_others() {
            let blob = footer_with_kv(&[("ARROW:schema", Some("xyz")), ("maml", Some("old"))]);
            let out = upsert_kv(&blob, "maml", "new").unwrap();
            // maml dropped then re-pushed at the end; the other key survives untouched
            assert_eq!(
                read_kv_pairs(&out).unwrap(),
                owned(&[("ARROW:schema", Some("xyz")), ("maml", Some("new"))])
            );
        }

        #[test]
        fn no_duplicate_maml_after_replace() {
            let blob = footer_with_kv(&[("maml", Some("old"))]);
            let out = upsert_kv(&blob, "maml", "new").unwrap();
            let pairs = read_kv_pairs(&out).unwrap();
            assert_eq!(pairs.iter().filter(|(k, _)| k == "maml").count(), 1);
            assert_eq!(pairs[0].1.as_deref(), Some("new"));
        }

        #[test]
        fn skips_leading_fields_to_find_field5() {
            let blob = footer_with_leading_field(&[("a", Some("1"))]);
            let out = upsert_kv(&blob, "maml", "v").unwrap();
            assert_eq!(
                read_kv_pairs(&out).unwrap(),
                owned(&[("a", Some("1")), ("maml", Some("v"))])
            );
        }

        #[test]
        fn handles_empty_kv_list() {
            let blob = footer_with_kv(&[]); // field 5 present but count 0
            let out = upsert_kv(&blob, "maml", "v").unwrap();
            assert_eq!(read_kv_pairs(&out).unwrap(), owned(&[("maml", Some("v"))]));
        }

        #[test]
        fn idempotent_re_tag() {
            // tag, then tag again — exercises reading back our OWN long-form field 5
            let blob = [ThriftID::Stop as u8];
            let once = upsert_kv(&blob, "maml", "v1").unwrap();
            let twice = upsert_kv(&once, "maml", "v2").unwrap();
            assert_eq!(
                read_kv_pairs(&twice).unwrap(),
                owned(&[("maml", Some("v2"))])
            );
        }

        #[test]
        fn rejects_field5_with_wrong_type() {
            // field 5 present but encoded as i64 instead of a list
            let mut blob = vec![ThriftID::I64 as u8, zigzag(5) as u8];
            write_uvarint(&mut blob, zigzag(7)); // some i64 value
            blob.push(ThriftID::Stop as u8);
            assert!(upsert_kv(&blob, "maml", "v").is_err());
        }
    }

    #[cfg(test)]
    mod test_zigzag {
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
    }

    #[cfg(test)]
    mod test_uvarint {
        use super::*;
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
            // 60k test
            let res_60k = read_uvarint(&buffer, &mut pos).unwrap();
            assert_eq!(res_60k, 60_000u64);
        }
    }
}
