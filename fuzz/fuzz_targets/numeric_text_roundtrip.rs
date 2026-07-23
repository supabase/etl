#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|value: &str| {
    etl::fuzzing::check_numeric_text_roundtrip(value);
});
