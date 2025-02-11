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
        use serde::Deserialize;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrNumber {
            String(String),
            Number(u64),
        }

        macro_rules! match_suffix {
            ($str: ident, $suf: expr, $variant: expr) => {
                if $str.ends_with($suf) {
                    let num = $str
                        .trim_end_matches($suf)
                        .parse()
                        .map_err(serde::de::Error::custom)?;
                    return Ok(($variant)(num));
                }
            };
        }

        let s_or_n = StringOrNumber::deserialize(deserializer)?;

        match s_or_n {
            StringOrNumber::String(str) => {
                match_suffix!(str, "TB", ByteCapacity::TB);
                match_suffix!(str, "GB", ByteCapacity::GB);
                match_suffix!(str, "MB", ByteCapacity::MB);
                match_suffix!(str, "KB", ByteCapacity::KB);
                match_suffix!(str, "B", ByteCapacity::B);

                let num = str.parse().map_err(|_| {
                    serde::de::Error::custom(
                        "Expect a number or a string with a TB|GB|MB|KB|B suffix",
                    )
                })?;
                Ok(ByteCapacity::B(num))
            }
            StringOrNumber::Number(num) => Ok(ByteCapacity::B(num)),
        }
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
        assert!(matches!(
            serde_json::from_str::<ByteCapacity>("5"),
            Ok(ByteCapacity::B(5))
        ));
        let err = serde_json::from_str::<ByteCapacity>("\"Wrong string format\"")
            .err()
            .expect("Expect an error");
        assert_eq!(
            err.to_string(),
            "Expect a number or a string with a TB|GB|MB|KB|B suffix"
        );
    }
}
