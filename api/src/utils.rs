use rand::{Rng, distributions::Slice};

/// Generates a random alphabetic string of length `len`
pub fn generate_random_alpha_str(len: usize) -> String {
    let chars = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let chars_dist = Slice::new(&chars).expect("passed slice was empty");
    let rng = rand::thread_rng();
    rng.sample_iter(&chars_dist).take(len).collect()
}
