use std::io::Read;

use byteorder::ReadBytesExt;
use serde::{Deserialize, Serialize};

use crate::{adapter::IntoClickhouseValue, errors};

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
            _ => Err(format!("Unknown message type: {value}")),
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
            _ => Err(format!("Unknown tuple type: {value}")),
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum PgOutputValue {
    Unit,
    #[default]
    Null,
    Unchanged,
    Text(String),
    Binary(Vec<u8>),
}

impl IntoClickhouseValue for PgOutputValue {
    fn to_integer(self) -> String {
        self.text_or("0".to_string())
    }

    fn to_real(self) -> String {
        self.text_or("0.0".to_string())
    }

    fn to_bool(self) -> String {
        Self::parse_bool(&self.text_or("false".to_string()))
    }

    fn to_string(self) -> String {
        format!("'{}'", Self::escape_string(&self.text_or("".to_string())))
    }

    fn to_date(self) -> String {
        format!(
            "toDate('{}')",
            Self::format_date_time(&self.text_or("current_date()".to_string()))
        )
    }

    fn to_datetime(self) -> String {
        format!(
            "toDateTime('{}')",
            Self::format_date_time(&self.text_or("now()".to_string()))
        )
    }

    fn to_time(self) -> String {
        format!(
            "toTime('{}')",
            Self::format_date_time(&self.text_or("now()".to_string()))
        )
    }

    fn to_array(self) -> String {
        format!("[{}]", self.array_value().unwrap_or_default(),)
    }

    fn to_string_array(self) -> String {
        let text = self.array_value().unwrap_or_default();
        let array_values = Self::parse_string_array(&text)
            .into_iter()
            .map(|s| format!("'{}'", Self::escape_string(&s)))
            .collect::<Vec<String>>();

        format!("[{}]", array_values.join(", "))
    }

    fn is_null(&self) -> bool {
        matches!(self, PgOutputValue::Null)
    }

    fn unknown_value(self) -> String {
        self.text_or("NULL".to_string())
    }

    fn into_null(self) -> Self {
        PgOutputValue::Null
    }
}

impl PgOutputValue {
    pub fn parse_bool(value: &str) -> String {
        match value.to_lowercase().as_str() {
            "t" | "1" | "true" => "TRUE".to_string(),
            "f" | "0" | "false" => "FALSE".to_string(),
            _ => "FALSE".to_string(),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, PgOutputValue::Null)
    }

    pub fn text_ref_or(&self, default: &'static str) -> &str {
        match self {
            PgOutputValue::Text(value) => value.as_str(),
            _ => default,
        }
    }

    pub fn text_or(self, default: String) -> String {
        match self {
            PgOutputValue::Text(value) => value,
            _ => default,
        }
    }

    pub fn array_value(&self) -> Option<String> {
        if let PgOutputValue::Text(value) = self {
            if value.starts_with('{') && value.ends_with('}') {
                Some(value[1..value.len() - 1].to_string())
            } else {
                Some(value.to_string())
            }
        } else {
            None
        }
    }

    pub fn escape_string(input: &str) -> String {
        input.replace('\'', "''").replace("\\", "\\\\")
    }

    pub fn parse_string_array(value: &str) -> Vec<String> {
        let value = value.trim_matches(|c| c == '{' || c == '}');

        let trimmed = value.trim_matches('"');
        let items: Vec<String> = trimmed.split("\",\"").map(|s| s.to_string()).collect();
        items
    }

    /*
    다앙한 형태의 datetime 타입을 '2025-08-18 03:56:32' 형태로 변환하는 함수
    입력 예시
    1. '2025-08-18 05:16:08.490845+00'
    2. '2025-08-18 05:16:08.860455'
    3. '2025-08-17 22:00:00+00'
    4. '2020-03-09'
    */
    pub fn format_date_time(source: &str) -> String {
        // .가 있는 경우 . 오른쪽을 잘라서 버립니다.
        let formatted = if let Some(pos) = source.find('.') {
            source[..pos].to_string()
        } else {
            source.to_string()
        };

        // +가 있는 경우 + 오른쪽을 잘라서 버립니다.
        if let Some(pos) = formatted.find('+') {
            formatted[..pos].to_string()
        } else {
            formatted
        }
    }
}

pub fn parse_pg_output(bytes: &[u8]) -> errors::Result<Option<PgOutput>> {
    let first_byte = bytes.first().cloned().unwrap_or(0);
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
    let mut cursor = std::io::Cursor::new(&bytes[1..]); // Skip the first byte (message type)

    let mut pg_output = PgOutput {
        message_type,
        relation_id: 0,
        tuple_type: None,
        payload: Vec::new(),
    };

    // Read relation ID (4 bytes)
    pg_output.relation_id = cursor.read_u32::<byteorder::BigEndian>().map_err(|e| {
        errors::Errors::PgOutputParseError(format!("Failed to read relation ID: {e}"))
    })?;

    match message_type {
        MessageType::Insert => {
            // Insert: relation_id + 'N' + tuple_data
            let tuple_type_byte = cursor.read_u8().map_err(|e| {
                errors::Errors::PgOutputParseError(format!("Failed to read tuple type: {e}"))
            })?;

            pg_output.tuple_type = Some(PgTupleType::try_from(tuple_type_byte).map_err(|e| {
                errors::Errors::PgOutputParseError(format!("Invalid tuple type: {e}"))
            })?);
        }
        MessageType::Update => {
            // Update: relation_id + ('K'|'O'|'N') + tuple_data
            let tuple_type_byte = cursor.read_u8().map_err(|e| {
                errors::Errors::PgOutputParseError(format!("Failed to read tuple type: {e}"))
            })?;

            pg_output.tuple_type = Some(PgTupleType::try_from(tuple_type_byte).map_err(|e| {
                errors::Errors::PgOutputParseError(format!("Invalid tuple type: {e}"))
            })?);
        }
        MessageType::Delete => {
            // Delete: relation_id + ('K'|'O') + tuple_data
            let tuple_type_byte = cursor.read_u8().map_err(|e| {
                errors::Errors::PgOutputParseError(format!("Failed to read tuple type: {e}"))
            })?;

            pg_output.tuple_type = Some(PgTupleType::try_from(tuple_type_byte).map_err(|e| {
                errors::Errors::PgOutputParseError(format!("Invalid tuple type: {e}"))
            })?);
        }
        MessageType::Truncate => {
            // Truncate: relation_id + flags + no tuple data
            let _flags = cursor.read_u8().map_err(|e| {
                errors::Errors::PgOutputParseError(format!("Failed to read truncate flags: {e}"))
            })?;
            // No tuple data for truncate
            return Ok(pg_output);
        }
        _ => {
            return Err(errors::Errors::PgOutputParseError(format!(
                "Unexpected message type for write operation: {message_type:?}"
            )));
        }
    }

    // Read column count (2 bytes)
    let column_count = cursor.read_u16::<byteorder::BigEndian>().map_err(|e| {
        errors::Errors::PgOutputParseError(format!("Failed to read column count: {e}"))
    })? as usize;

    pg_output.payload = Vec::with_capacity(column_count);

    // Parse columns
    for _i in 0..column_count {
        let column_type = cursor.read_u8().map_err(|e| {
            errors::Errors::PgOutputParseError(format!("Failed to read column type: {e}"))
        })?;

        match column_type {
            b'n' => {
                // NULL value
                pg_output.payload.push(PgOutputValue::Null);
            }
            b'u' => {
                // UNCHANGED value (for UPDATE) - skip
                pg_output.payload.push(PgOutputValue::Unchanged);
            }
            b't' => {
                // Text value
                let length = cursor.read_u32::<byteorder::BigEndian>().map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Failed to read text length: {e}"))
                })?;

                let mut buffer = vec![0u8; length as usize];

                cursor.read_exact(&mut buffer).map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Failed to read text value: {e}"))
                })?;

                let text_value = String::from_utf8(buffer).map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Invalid UTF-8 sequence: {e}"))
                })?;

                pg_output.payload.push(PgOutputValue::Text(text_value));
            }
            b'b' => {
                // Binary value
                let length = cursor.read_u32::<byteorder::BigEndian>().map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Failed to read binary length: {e}"))
                })?;

                let mut buffer = vec![0u8; length as usize];

                cursor.read_exact(&mut buffer).map_err(|e| {
                    errors::Errors::PgOutputParseError(format!("Failed to read binary value: {e}"))
                })?;

                pg_output.payload.push(PgOutputValue::Binary(buffer));
            }
            _ => {
                return Err(errors::Errors::PgOutputParseError(format!(
                    "Unknown column type: {} (0x{:02x})",
                    column_type as char, column_type
                )));
            }
        }
    }

    Ok(pg_output)
}

#[cfg(test)]
mod tests {
    use crate::adapter::postgres::pgoutput::PgOutputValue;

    #[test]
    fn test_parse_string_array() {
        struct TestCase {
            input: &'static str,
            expected: Vec<String>,
        }

        let test_cases = vec![
            TestCase {
                input: "{\"Flower design\",\"Pearl embellishments\",\"Stud earrings\",\"Gold accents\",\"Pearl accents\",\"Diamond accents\"}",
                expected: vec![
                    "Flower design".to_string(),
                    "Pearl embellishments".to_string(),
                    "Stud earrings".to_string(),
                    "Gold accents".to_string(),
                    "Pearl accents".to_string(),
                    "Diamond accents".to_string(),
                ],
            },
            TestCase {
                input: "{\"Button closure\",\"White stripes on collar, cuffs, and hem\"}",
                expected: vec![
                    "Button closure".to_string(),
                    "White stripes on collar, cuffs, and hem".to_string(),
                ],
            },
        ];

        for test_case in test_cases {
            let result = PgOutputValue::parse_string_array(test_case.input);
            assert_eq!(
                result, test_case.expected,
                "Failed for input: {}",
                test_case.input
            );
        }
    }

    #[test]
    fn test_format_date_time() {
        struct TestCase {
            input: &'static str,
            expected: &'static str,
        }

        let test_cases = vec![
            TestCase {
                input: "2025-08-18 05:16:08.490845+00",
                expected: "2025-08-18 05:16:08",
            },
            TestCase {
                input: "2025-08-18 05:16:08.860455",
                expected: "2025-08-18 05:16:08",
            },
            TestCase {
                input: "2025-08-17 22:00:00+00",
                expected: "2025-08-17 22:00:00",
            },
        ];

        for test_case in test_cases {
            let result = PgOutputValue::format_date_time(test_case.input);
            assert_eq!(
                result, test_case.expected,
                "Failed for input: {}",
                test_case.input
            );
        }
    }
}
