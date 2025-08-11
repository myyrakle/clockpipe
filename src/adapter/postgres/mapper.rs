use crate::adapter::postgres::PostgresColumnType;

pub fn convert_postgres_column_type_to_clickhouse(
    postgres_column_type: &PostgresColumnType,
) -> String {
    match postgres_column_type.data_type.as_str() {
        "int2" => {
            if postgres_column_type.nullable {
                "Nullable(Int16)".to_string()
            } else {
                "Int16".to_string()
            }
        }
        "_int2" => "Array(Int16)".to_string(),
        "int4" | "int" => {
            if postgres_column_type.nullable {
                "Nullable(Int32)".to_string()
            } else {
                "Int32".to_string()
            }
        }
        "_int4" => "Array(Int32)".to_string(),
        "int8" => {
            if postgres_column_type.nullable {
                "Nullable(Int64)".to_string()
            } else {
                "Int64".to_string()
            }
        }
        "_int8" => "Array(Int64)".to_string(),
        "float4" => {
            if postgres_column_type.nullable {
                "Nullable(Float32)".to_string()
            } else {
                "Float32".to_string()
            }
        }
        "_float4" => "Array(Float32)".to_string(),
        "float8" => {
            if postgres_column_type.nullable {
                "Nullable(Float64)".to_string()
            } else {
                "Float64".to_string()
            }
        }
        "_float8" => "Array(Float64)".to_string(),
        "numeric" => {
            if postgres_column_type.nullable {
                "Nullable(Decimal)".to_string()
            } else {
                "Decimal".to_string()
            }
        }
        "_numeric" => "Array(Decimal)".to_string(),
        // varchar
        "varchar" | "text" | "json" | "jsonb" => {
            if postgres_column_type.nullable {
                "Nullable(String)".to_string()
            } else {
                "String".to_string()
            }
        }
        "_varchar" => "Array(String)".to_string(),
        "_text" => "Array(String)".to_string(),
        // Boolean
        "bool" => {
            if postgres_column_type.nullable {
                "Nullable(Bool)".to_string()
            } else {
                "Bool".to_string()
            }
        }
        "_bool" => "Array(Bool)".to_string(),
        // time
        "timestamp" | "timestamptz" => {
            if postgres_column_type.nullable {
                "Nullable(DateTime)".to_string()
            } else {
                "DateTime".to_string()
            }
        }
        "date" => {
            if postgres_column_type.nullable {
                "Nullable(Date)".to_string()
            } else {
                "Date".to_string()
            }
        }
        _ => {
            log::warn!(
                "Unsupported Postgres data type: {}. Defaulting to String.",
                postgres_column_type.data_type
            );

            if postgres_column_type.nullable {
                "Nullable(String)".to_string()
            } else {
                "String".to_string()
            }
        }
    }
}
