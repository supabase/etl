#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (u8, &str)| {
    let (selector, value) = input;
    let _ = etl::fuzzing::parse_text_cell(selector, value);
});
