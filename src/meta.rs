//! Free-form, wire-safe metadata that a sender can attach to a patch batch.
//!
//! [`Value`] is a small, self-contained dynamic value type (similar in spirit to
//! `serde_json::Value`) that round-trips through postcard. It is externally
//! tagged on purpose: postcard is not self-describing, so `#[serde(untagged)]`
//! would not deserialize.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::snapshot::SnapshotEntry;

/// String-keyed map of metadata values.
pub type Metadata = BTreeMap<String, Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(Metadata),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns the value as `i64`, converting from `U64` when it fits.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(n) => Some(*n),
            Self::U64(n) => i64::try_from(*n).ok(),
            _ => None,
        }
    }

    /// Returns the value as `u64`, converting from non-negative `I64`.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(n) => Some(*n),
            Self::I64(n) => u64::try_from(*n).ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F64(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Self::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&Metadata> {
        match self {
            Self::Map(m) => Some(m),
            _ => None,
        }
    }
}

/// Supplies per-event metadata on the sender side.
///
/// Every method defaults to an empty map, so only override what you need.
/// The receiver gets each map on the matching `RecvEvent`:
///
/// | method       | event                                   |
/// |--------------|-----------------------------------------|
/// | `pre_apply`  | `EntryPreApply` (once per batch)        |
/// | `receiving`  | `EntryReceiving` (once per entry)       |
/// | `applied`    | `EntryApplied` (once per entry)         |
/// | `post_apply` | `EntryPostApply` (once per batch)       |
///
/// All methods run on the sender after the diff is computed and before the
/// patch is sent, in this order: `pre_apply`, then `receiving` and `applied`
/// for each entry in diff order, then `post_apply`. They describe what the
/// sender knows and can't reflect receiver-side outcomes. None of them are
/// called when there are no changes to send.
pub trait MetadataProvider: Send + 'static {
    fn pre_apply(&mut self, entries: &[SnapshotEntry]) -> Metadata {
        let _ = entries;
        Metadata::new()
    }

    fn receiving(&mut self, entry: &SnapshotEntry) -> Metadata {
        let _ = entry;
        Metadata::new()
    }

    fn applied(&mut self, entry: &SnapshotEntry) -> Metadata {
        let _ = entry;
        Metadata::new()
    }

    fn post_apply(&mut self, entries: &[SnapshotEntry]) -> Metadata {
        let _ = entries;
        Metadata::new()
    }
}

macro_rules! impl_from {
    ($($t:ty => $variant:ident $(as $cast:ty)?),* $(,)?) => {
        $(
            impl From<$t> for Value {
                fn from(v: $t) -> Self {
                    Self::$variant(v $(as $cast)?)
                }
            }
        )*
    };
}

impl_from! {
    bool => Bool,
    i64 => I64,
    i32 => I64 as i64,
    u64 => U64,
    u32 => U64 as u64,
    f64 => F64,
    String => String,
    Vec<u8> => Bytes,
    Vec<Value> => List,
    Metadata => Map,
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Self::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Metadata {
        let mut inner = Metadata::new();
        inner.insert("nested".into(), Value::from(true));

        let mut m = Metadata::new();
        m.insert("null".into(), Value::Null);
        m.insert("neg".into(), Value::from(-5i64));
        m.insert("pos".into(), Value::from(42u64));
        m.insert("float".into(), Value::from(1.5f64));
        m.insert("str".into(), Value::from("hello"));
        m.insert("bytes".into(), Value::from(vec![0u8, 1, 255]));
        m.insert(
            "list".into(),
            Value::from(vec![Value::from(1u32), Value::from("x"), Value::Null]),
        );
        m.insert("map".into(), Value::from(inner));
        m
    }

    #[test]
    fn postcard_roundtrip() {
        let m = sample();
        let bytes = postcard::to_stdvec(&m).unwrap();
        let back: Metadata = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn empty_map_is_one_byte() {
        let bytes = postcard::to_stdvec(&Metadata::new()).unwrap();
        assert_eq!(bytes.len(), 1);
    }

    #[test]
    fn accessors() {
        let m = sample();
        assert!(m["null"].is_null());
        assert_eq!(m["neg"].as_i64(), Some(-5));
        assert_eq!(m["neg"].as_u64(), None);
        assert_eq!(m["pos"].as_u64(), Some(42));
        assert_eq!(m["pos"].as_i64(), Some(42));
        assert_eq!(m["float"].as_f64(), Some(1.5));
        assert_eq!(m["str"].as_str(), Some("hello"));
        assert_eq!(m["bytes"].as_bytes(), Some(&[0u8, 1, 255][..]));
        assert_eq!(m["list"].as_list().map(<[_]>::len), Some(3));
        assert_eq!(
            m["map"].as_map().and_then(|m| m["nested"].as_bool()),
            Some(true)
        );
        assert_eq!(m["str"].as_i64(), None);
    }
}
