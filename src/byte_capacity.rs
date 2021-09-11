pub const KILOBYTE: u64 = 1000;
pub const MEGABYTE: u64 = 1000 * KILOBYTE;
pub const GIGABYTE: u64 = 1000 * MEGABYTE;
pub const TERABYTE: u64 = 1000 * GIGABYTE;

pub enum ByteCapacity {
    B(u64),
    KB(u64),
    MB(u64),
    GB(u64),
    TB(u64),
}

impl ByteCapacity {
    pub fn bytes(&self) -> u64 {
        match self {
            ByteCapacity::B(x) => *x,
            ByteCapacity::KB(x) => x * KILOBYTE,
            ByteCapacity::MB(x) => x * MEGABYTE,
            ByteCapacity::GB(x) => x * GIGABYTE,
            ByteCapacity::TB(x) => x * TERABYTE,
        }
    }
}
