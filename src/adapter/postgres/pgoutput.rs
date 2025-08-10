use std::io::Read;

use byteorder::ReadBytesExt;
use serde::{Deserialize, Serialize};

use crate::errors;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    Begin = b'B',
    Commit = b'C',
    Origin = b'O',
    Relation = b'R',
    Type = b'Y',
    Insert = b'I',
    Update = b'U',
    Delete = b'D',
    Truncate = b'T',
}

impl TryFrom<u8> for MessageType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'B' => Ok(MessageType::Begin),
            b'C' => Ok(MessageType::Commit),
            b'O' => Ok(MessageType::Origin),
            b'R' => Ok(MessageType::Relation),
            b'Y' => Ok(MessageType::Type),
            b'I' => Ok(MessageType::Insert),
            b'U' => Ok(MessageType::Update),
            b'D' => Ok(MessageType::Delete),
            b'T' => Ok(MessageType::Truncate),
            _ => Err(format!("Unknown message type: {}", value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PgTupleType {
    Key = b'K',
    Old = b'O',
    New = b'N',
}

impl TryFrom<u8> for PgTupleType {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'N' => Ok(PgTupleType::New),
            b'K' => Ok(PgTupleType::Key),
            b'O' => Ok(PgTupleType::Old),
            _ => Err(format!("Unknown tuple type: {}", value)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgOutput {
    pub message_type: MessageType,
    pub relation_id: u32,
    pub tuple_type: Option<PgTupleType>,
    pub payload: Vec<PgOutputValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PgOutputValue {
    Unit,
    Null,
    Unchanged,
    Text(String),
    Binary(Vec<u8>),
}

pub fn parse_pg_output(bytes: &[u8]) -> errors::Result<Option<PgOutput>> {
    let first_byte = bytes.get(0).cloned().unwrap_or(0);
    let message_type =
        MessageType::try_from(first_byte).expect("Failed to parse message type from bytes");

    match message_type {
        MessageType::Begin => {
            // Handle Begin message
            Ok(None)
        }
        MessageType::Commit => {
            // Handle Commit message
            Ok(None)
        }
        MessageType::Origin => {
            // Handle Origin message
            Ok(None)
        }
        MessageType::Relation => {
            // Handle Relation message
            Ok(None)
        }
        MessageType::Type => {
            // Handle Type message
            Ok(None)
        }
        MessageType::Insert | MessageType::Update | MessageType::Delete | MessageType::Truncate => {
            let pg_output = parse_pg_output_write(message_type, bytes)?;
            Ok(Some(pg_output))
        }
    }
}

fn parse_pg_output_write(message_type: MessageType, bytes: &[u8]) -> errors::Result<PgOutput> {
    let mut cursor = std::io::Cursor::new(bytes);

    let mut pg_output = PgOutput {
        message_type,
        relation_id: 0,
        tuple_type: None,
        payload: Vec::new(),
    };

    // Read relation ID
    pg_output.relation_id = cursor.read_u32::<byteorder::BigEndian>().map_err(|e| {
        errors::Errors::PgOutputParseError(format!("Failed to read relation ID: {}", e))
    })?;

    // Read tuple type
    let tuple_type_byte = cursor.read_u8().map_err(|e| {
        errors::Errors::PgOutputParseError(format!("Failed to read tuple type: {}", e))
    })?;

    pg_output.tuple_type =
        Some(PgTupleType::try_from(tuple_type_byte).map_err(|e| {
            errors::Errors::PgOutputParseError(format!("Invalid tuple type: {}", e))
        })?);

    // Read column count
    let column_count = cursor.read_u16::<byteorder::BigEndian>().map_err(|e| {
        errors::Errors::PgOutputParseError(format!("Failed to read column count: {}", e))
    })? as usize;

    pg_output.payload = vec![PgOutputValue::Unit; column_count as usize];

    // Parse columns
    for i in 0..column_count {
        let column_type = cursor.read_u8().map_err(|e| {
            errors::Errors::PgOutputParseError(format!("Failed to read column type: {}", e))
        })?;

        match column_type {
            b'n' => {
                // NULL value
                pg_output.payload[i] = PgOutputValue::Null;
            }
            b'u' => {
                // UNCHANGED value (for UPDATE) - skip
                pg_output.payload[i] = PgOutputValue::Unchanged;
            }
            b't' => {
                // Text value
                let length = cursor.read_u32::<byteorder::BigEndian>().map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Failed to read text length: {}", e))
                })?;

                let mut buffer = vec![0u8; length as usize];

                cursor.read_exact(&mut buffer).map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Failed to read text value: {}", e))
                })?;

                let text_value = String::from_utf8(buffer).map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Invalid UTF-8 sequence: {}", e))
                })?;

                pg_output.payload[i] = PgOutputValue::Text(text_value);
            }
            b'b' => {
                // Binary value
                let length = cursor.read_u32::<byteorder::BigEndian>().map_err(|e| {
                    errors::Errors::PgOutputParseError(format!(
                        "Failed to read binary length: {}",
                        e
                    ))
                })?;

                let mut buffer = vec![0u8; length as usize];

                cursor.read_exact(&mut buffer).map_err(|e| {
                    errors::Errors::PgOutputParseError(format!(
                        "Failed to read binary value: {}",
                        e
                    ))
                })?;

                pg_output.payload[i] = PgOutputValue::Binary(buffer);
            }
            _ => {
                return Err(errors::Errors::PgOutputParseError(format!(
                    "Unknown column type: {}",
                    column_type
                )));
            }
        }
    }

    Ok(pg_output)
}
