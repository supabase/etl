#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|value: &str| {
    let _ = etl::fuzzing::parse_bytea_hex_string(value);
});
