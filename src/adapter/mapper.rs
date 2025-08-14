use crate::adapter::clickhouse::ClickhouseType;

pub trait IntoClickhouseColumn {
    fn into_clickhouse_type(&self) -> ClickhouseType;
}
