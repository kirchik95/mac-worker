//! Claude project directory encoding, following herdr-gpui Teleport (Apache-2.0).
pub fn claude_project_dir(cwd: &str) -> String {
    let units: Vec<u16> = cwd.encode_utf16().collect();
    let encoded: String = units
        .iter()
        .map(|&unit| {
            if unit <= 127 && (unit as u8).is_ascii_alphanumeric() {
                unit as u8 as char
            } else {
                '-'
            }
        })
        .collect();
    if encoded.len() <= 200 {
        return encoded;
    }
    let hash = units.iter().fold(0i32, |hash, &unit| {
        hash.wrapping_mul(31).wrapping_add(unit as i32)
    });
    let mut number = hash.unsigned_abs();
    let mut digits = Vec::new();
    loop {
        digits.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(number % 36) as usize] as char);
        number /= 36;
        if number == 0 {
            break;
        }
    }
    let suffix: String = digits.iter().rev().collect();
    format!("{}-{suffix}", &encoded[..200])
}
