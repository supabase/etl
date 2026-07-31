#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = etl::fuzzing::parse_copy_row(data);
});
