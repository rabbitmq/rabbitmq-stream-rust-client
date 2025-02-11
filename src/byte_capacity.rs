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

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ByteCapacity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Visitor;
        use std::fmt;

        struct U32OrStringVisitor;

        impl Visitor<'_> for U32OrStringVisitor {
            type Value = ByteCapacity;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a u64 or a \\d+(B|KB|MB|GB|TB) string")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(ByteCapacity::B(v))
            }

            fn visit_str<E>(self, str: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if str.ends_with("TB") {
                    let num = str
                        .trim_end_matches("TB")
                        .parse()
                        .map_err(serde::de::Error::custom)?;
                    Ok(ByteCapacity::TB(num))
                } else if str.ends_with("GB") {
                    let num = str
                        .trim_end_matches("GB")
                        .parse()
                        .map_err(serde::de::Error::custom)?;
                    Ok(ByteCapacity::GB(num))
                } else if str.ends_with("MB") {
                    let num = str
                        .trim_end_matches("MB")
                        .parse()
                        .map_err(serde::de::Error::custom)?;
                    Ok(ByteCapacity::MB(num))
                } else if str.ends_with("KB") {
                    let num = str
                        .trim_end_matches("KB")
                        .parse()
                        .map_err(serde::de::Error::custom)?;
                    Ok(ByteCapacity::KB(num))
                } else if str.ends_with("B") {
                    let num = str
                        .trim_end_matches("B")
                        .parse()
                        .map_err(serde::de::Error::custom)?;
                    Ok(ByteCapacity::B(num))
                } else {
                    let num = str.parse().map_err(serde::de::Error::custom)?;
                    Ok(ByteCapacity::B(num))
                }
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_any(U32OrStringVisitor)
    }
}

#[cfg(feature = "serde")]
mod tests {
    #[test]
    fn test_deserilize_byte_capacity() {
        use crate::types::ByteCapacity;

        assert!(matches!(
            serde_json::from_str::<ByteCapacity>("\"5GB\""),
            Ok(ByteCapacity::GB(5))
        ));
        assert!(matches!(
            serde_json::from_str::<ByteCapacity>("\"5TB\""),
            Ok(ByteCapacity::TB(5))
        ));
        assert!(matches!(
            serde_json::from_str::<ByteCapacity>("\"5MB\""),
            Ok(ByteCapacity::MB(5))
        ));
        assert!(matches!(
            serde_json::from_str::<ByteCapacity>("\"5KB\""),
            Ok(ByteCapacity::KB(5))
        ));
        assert!(matches!(
            serde_json::from_str::<ByteCapacity>("\"5B\""),
            Ok(ByteCapacity::B(5))
        ));
        assert!(matches!(
            serde_json::from_str::<ByteCapacity>("\"5\""),
            Ok(ByteCapacity::B(5))
        ));
    }
}
