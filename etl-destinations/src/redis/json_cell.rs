use etl::types::{ArrayCell, Cell, PgNumeric, Sign};
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};

pub(super) enum JsonCell<'a> {
    Value(Cell),
    Ref(&'a Cell),
}

impl<'a> AsRef<Cell> for JsonCell<'a> {
    fn as_ref(&self) -> &Cell {
        match &self {
            JsonCell::Value(cell) => cell,
            JsonCell::Ref(cell) => cell,
        }
    }
}

impl<'a> Serialize for JsonCell<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.as_ref() {
            Cell::Null => serializer.serialize_none(),
            Cell::Bool(b) => b.serialize(serializer),
            Cell::String(string) => string.serialize(serializer),
            Cell::I16(number) => number.serialize(serializer),
            Cell::I32(number) => number.serialize(serializer),
            Cell::U32(number) => number.serialize(serializer),
            Cell::I64(number) => number.serialize(serializer),
            Cell::F32(number) => number.serialize(serializer),
            Cell::F64(number) => number.serialize(serializer),
            Cell::Numeric(pg_numeric) => match pg_numeric {
                etl::types::PgNumeric::NaN => serializer.serialize_none(),
                etl::types::PgNumeric::PositiveInfinity => serializer.serialize_none(),
                etl::types::PgNumeric::NegativeInfinity => serializer.serialize_none(),
                etl::types::PgNumeric::Value { sign, .. } => match sign {
                    Sign::Positive => pg_numeric
                        .to_string()
                        .parse::<u64>()
                        .map_err(|err| {
                            serde::ser::Error::custom(format!(
                                "cannot parse pg numeric to u64: {err}"
                            ))
                        })?
                        .serialize(serializer),
                    Sign::Negative => pg_numeric
                        .to_string()
                        .parse::<i64>()
                        .map_err(|err| {
                            serde::ser::Error::custom(format!(
                                "cannot parse pg numeric to i64: {err}"
                            ))
                        })?
                        .serialize(serializer),
                },
            },
            Cell::Date(naive_date) => naive_date.serialize(serializer),
            Cell::Time(naive_time) => naive_time.serialize(serializer),
            Cell::Timestamp(naive_date_time) => naive_date_time.serialize(serializer),
            Cell::TimestampTz(date_time) => date_time.serialize(serializer),
            Cell::Uuid(uuid) => uuid.serialize(serializer),
            Cell::Json(value) => value.serialize(serializer),
            Cell::Bytes(items) => items.serialize(serializer),
            Cell::Array(array_cell) => match array_cell {
                ArrayCell::Bool(items) => items.serialize(serializer),
                ArrayCell::String(items) => items.serialize(serializer),
                ArrayCell::I16(items) => items.serialize(serializer),
                ArrayCell::I32(items) => items.serialize(serializer),
                ArrayCell::U32(items) => items.serialize(serializer),
                ArrayCell::I64(items) => items.serialize(serializer),
                ArrayCell::F32(items) => items.serialize(serializer),
                ArrayCell::F64(items) => items.serialize(serializer),
                ArrayCell::Numeric(pg_numerics) => {
                    let mut seq = serializer.serialize_seq(Some(pg_numerics.len()))?;
                    for elt in pg_numerics {
                        match elt {
                            Some(PgNumeric::NaN)
                            | Some(PgNumeric::NegativeInfinity)
                            | Some(PgNumeric::PositiveInfinity) => {
                                seq.serialize_element::<Option<i64>>(&None)?
                            }
                            Some(PgNumeric::Value { sign, .. }) => match sign {
                                Sign::Positive => {
                                    seq.serialize_element(
                                        &elt.as_ref().unwrap().to_string().parse::<u64>().map_err(
                                            |err| {
                                                serde::ser::Error::custom(format!(
                                                    "cannot parse pg numeric to u64: {err}"
                                                ))
                                            },
                                        )?,
                                    )?;
                                }
                                Sign::Negative => {
                                    seq.serialize_element(
                                        &elt.as_ref().unwrap().to_string().parse::<i64>().map_err(
                                            |err| {
                                                serde::ser::Error::custom(format!(
                                                    "cannot parse pg numeric to i64: {err}"
                                                ))
                                            },
                                        )?,
                                    )?;
                                }
                            },
                            None => {
                                seq.serialize_element::<Option<i64>>(&None)?;
                            }
                        }
                    }
                    seq.end()
                }
                ArrayCell::Date(naive_dates) => naive_dates.serialize(serializer),
                ArrayCell::Time(naive_times) => naive_times.serialize(serializer),
                ArrayCell::Timestamp(naive_date_times) => naive_date_times.serialize(serializer),
                ArrayCell::TimestampTz(date_times) => date_times.serialize(serializer),
                ArrayCell::Uuid(uuids) => uuids.serialize(serializer),
                ArrayCell::Json(values) => values.serialize(serializer),
                ArrayCell::Bytes(items) => items.serialize(serializer),
            },
        }
    }
}
