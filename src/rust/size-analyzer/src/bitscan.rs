/// Calculate the required bits to represent an integer (in Realm's array
/// integer encoding).
///
/// This algorithm is uses an `lz` (leading zeros) instruction and a lookup
/// table, which is faster (at least on Apple M1 Pro) than the algorithm in
/// `Array::bit_width` (array.cpp:193).
pub fn bit_width(value: i64) -> usize {
    let mut v = value as u64;

    if v >> 4 == 0 {
        static BITS: [u8; 16] = [0, 1, 2, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4];
        return BITS[v as usize] as usize;
    }

    if value < 0 {
        v = !v;
    }

    let leading_zeros = v.leading_zeros();

    #[rustfmt::skip]
    const LOOKUP: [u8; 65] = [
        // Upper 32 bits need 64 bits.
        64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64,

        // Bits 17..=32 need 32 bits.
        64, 32, 32, 32, 32, 32, 32, 32,
        32, 32, 32, 32, 32, 32, 32, 32,

        // Bits 9..=16 need 16 bits.
        32, 16, 16, 16, 16, 16, 16, 16,
        // Bits 0..=8 need 8 bits, because non-negative small numbers are handled above.
        16, 8, 8, 8,
        8, 8, 8, 8,
        8,
    ];
    LOOKUP[leading_zeros as usize] as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitscan_lz_lookup2() {
        assert_eq!(bit_width(0), 0);
        assert_eq!(bit_width(1), 1);
        assert_eq!(bit_width(2), 2);
        assert_eq!(bit_width(3), 2);
        assert_eq!(bit_width(4), 4);
        assert_eq!(bit_width(5), 4);
        assert_eq!(bit_width(6), 4);
        assert_eq!(bit_width(7), 4);
        assert_eq!(bit_width(8), 4);
        assert_eq!(bit_width(15), 4);
        assert_eq!(bit_width(16), 8);
        assert_eq!(bit_width(127), 8);
        assert_eq!(bit_width(128), 16);
        assert_eq!(bit_width(32767), 16);
        assert_eq!(bit_width(32768), 32);
        assert_eq!(bit_width(2147483647), 32);
        assert_eq!(bit_width(2147483648), 64);

        assert_eq!(bit_width(-1), 8);
        assert_eq!(bit_width(-2), 8);
        assert_eq!(bit_width(-3), 8);
        assert_eq!(bit_width(-4), 8);
        assert_eq!(bit_width(-5), 8);
        assert_eq!(bit_width(-6), 8);
        assert_eq!(bit_width(-7), 8);
        assert_eq!(bit_width(-8), 8);
        assert_eq!(bit_width(-15), 8);
        assert_eq!(bit_width(-16), 8);
        assert_eq!(bit_width(-127), 8);
        assert_eq!(bit_width(-128), 8);
        assert_eq!(bit_width(-129), 16);
        assert_eq!(bit_width(-32768), 16);
        assert_eq!(bit_width(-32769), 32);
        assert_eq!(bit_width(-2147483648), 32);
        assert_eq!(bit_width(-2147483649), 64);
    }
}
