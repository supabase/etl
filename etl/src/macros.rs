#[macro_export]
macro_rules! etl_error {
    ($kind:expr, $desc:expr) => {
        EtlError::from(($kind, $desc))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        EtlError::from(($kind, $desc, $detail.to_string()))
    };
}

#[macro_export]
macro_rules! bail {
    ($kind:expr, $desc:expr) => {
        return Err($crate::etl_error!($kind, $desc))
    };
    ($kind:expr, $desc:expr, $detail:expr) => {
        return Err($crate::etl_error!($kind, $desc, $detail))
    };
}
