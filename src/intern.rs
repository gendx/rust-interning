use crate::size::EstimateSize;
use appendvec::AppendVec;
use dashtable::DashTable;
use hashbrown::DefaultHashBuilder;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;

pub type IString = Interned<str>;
pub type StringInterner = Interner<str>;

pub struct Interned<T: ?Sized> {
    id: u32,
    _phantom: PhantomData<fn() -> T>,
}

impl<T: ?Sized> Debug for Interned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<T: ?Sized> PartialEq for Interned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T: ?Sized> Eq for Interned<T> {}

impl<T: ?Sized> PartialOrd for Interned<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: ?Sized> Ord for Interned<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<T: ?Sized> Hash for Interned<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T: ?Sized> EstimateSize for Interned<T> {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl<T: ?Sized> Interned<T> {
    pub(crate) fn from_id(id: u32) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn id(&self) -> u32 {
        self.id
    }
}

impl<T: ?Sized + Eq + Hash> Interned<T> {
    pub fn from(interner: &Interner<T>, value: impl Borrow<T> + Into<Arc<T>>) -> Self {
        let id = interner.intern(value);
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized> Interned<T> {
    #[expect(dead_code)]
    pub fn lookup(&self, interner: &Interner<T>) -> Arc<T> {
        interner.lookup(self.id)
    }

    pub fn lookup_ref<'a>(&self, interner: &'a Interner<T>) -> &'a T {
        interner.lookup_ref(self.id)
    }
}

pub trait EqWith<Rhs: ?Sized, Helper: ?Sized> {
    fn eq_with(&self, other: &Rhs, helper: &Helper) -> bool;
}

impl<T: ?Sized + Eq + Hash> EqWith<T, Interner<T>> for Interned<T> {
    fn eq_with(&self, other: &T, interner: &Interner<T>) -> bool {
        self.lookup_ref(interner) == other
    }
}

impl<T: ?Sized + Eq + Hash> Interned<T> {
    pub fn eq_with_more<U, Helper>(
        &self,
        other: &U,
        interner: &Interner<T>,
        helper: &Helper,
    ) -> bool
    where
        T: EqWith<U, Helper>,
    {
        self.lookup_ref(interner).eq_with(other, helper)
    }
}

impl<T: ?Sized> Serialize for Interned<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.id)
    }
}

impl<'de, T: ?Sized> Deserialize<'de> for Interned<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let id = deserializer.deserialize_u32(U32Visitor)?;
        Ok(Self {
            id,
            _phantom: PhantomData,
        })
    }
}

struct U32Visitor;

impl Visitor<'_> for U32Visitor {
    type Value = u32;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an integer between 0 and 2^32")
    }

    fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(u32::from(value))
    }

    fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(u32::from(value))
    }

    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value
            .try_into()
            .map_err(|_| E::custom(format!("u32 out of range: {}", value)))
    }
}

pub struct Interner<T: ?Sized> {
    vec: AppendVec<Arc<T>>,
    map: DashTable<u32>,
    hasher: DefaultHashBuilder,
    references: AtomicUsize,
}

impl<T: ?Sized> Default for Interner<T> {
    fn default() -> Self {
        Self {
            vec: AppendVec::new(),
            map: DashTable::new(),
            hasher: DefaultHashBuilder::default(),
            references: AtomicUsize::new(0),
        }
    }
}

impl<T: ?Sized + Debug> Debug for Interner<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_list().entries(self.vec.iter()).finish()
    }
}

impl<T: ?Sized + Eq + Hash> PartialEq for Interner<T> {
    fn eq(&self, other: &Self) -> bool {
        self.vec.iter().eq(other.vec.iter())
    }
}

impl<T: ?Sized + Eq + Hash> Eq for Interner<T> {}

impl<T: ?Sized + EstimateSize> EstimateSize for Interner<T> {
    fn allocated_bytes(&self) -> usize {
        self.vec.iter().map(|x| x.estimated_bytes()).sum::<usize>()
            + self.vec.len() * size_of::<u32>()
    }
}

impl<T: ?Sized + EstimateSize> Interner<T> {
    pub fn print_summary(&self, prefix: &str, title: &str, total_bytes: usize) {
        let len = self.len();
        let references = self.references();
        let estimated_bytes = self.estimated_bytes();
        println!(
            "{}- [{:.02}%] {} interner: {} objects | {} bytes ({:.02} bytes/object) | {} references ({:.02} refs/object)",
            prefix,
            estimated_bytes as f64 * 100.0 / total_bytes as f64,
            title,
            len,
            estimated_bytes,
            estimated_bytes as f64 / len as f64,
            references,
            references as f64 / len as f64,
        );
    }
}

impl<T: ?Sized> Interner<T> {
    fn len(&self) -> usize {
        self.vec.len()
    }

    fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<T: ?Sized + Eq + Hash> Interner<T> {
    fn intern(&self, value: impl Borrow<T> + Into<Arc<T>>) -> u32 {
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value.borrow());
        *self
            .map
            .entry(
                hash,
                |&i| self.vec[i as usize].deref() == value.borrow(),
                |&i| self.hasher.hash_one(self.vec[i as usize].deref()),
            )
            .or_insert_with(|| {
                let arc: Arc<T> = value.into();
                let id = self.vec.push(arc);
                assert!(id <= u32::MAX as usize);
                id as u32
            })
            .get()
    }

    /// Unconditionally push a value, without validating that it's already interned.
    fn push(&mut self, value: Arc<T>) -> u32 {
        let hash = self.hasher.hash_one(value.deref());

        let id = self.vec.push_mut(Arc::clone(&value));
        assert!(id <= u32::MAX as usize);
        let id = id as u32;

        self.map.insert_unique(hash, id, |&i| {
            self.hasher.hash_one(self.vec[i as usize].deref())
        });

        id
    }
}

impl<T: ?Sized> Interner<T> {
    fn lookup(&self, id: u32) -> Arc<T> {
        Arc::clone(&self.vec[id as usize])
    }

    fn lookup_ref(&self, id: u32) -> &T {
        self.vec[id as usize].deref()
    }
}

impl<T: ?Sized + Serialize> Serialize for Interner<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.vec.iter().map(|arc| arc.deref()))
    }
}

impl<'de, T> Deserialize<'de> for Interner<T>
where
    T: ?Sized + Eq + Hash,
    Arc<T>: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(InternerVisitor::new())
    }
}

struct InternerVisitor<T: ?Sized> {
    _phantom: PhantomData<fn() -> Interner<T>>,
}

impl<T: ?Sized> InternerVisitor<T> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<'de, T> Visitor<'de> for InternerVisitor<T>
where
    T: ?Sized + Eq + Hash,
    Arc<T>: Deserialize<'de>,
{
    type Value = Interner<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence of values")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut interner = match seq.size_hint() {
            None => Interner::default(),
            Some(size_hint) => Interner {
                vec: AppendVec::with_capacity(size_hint),
                map: DashTable::with_capacity(size_hint),
                hasher: DefaultHashBuilder::default(),
                references: AtomicUsize::new(0),
            },
        };

        while let Some(t) = seq.next_element()? {
            interner.push(t);
        }

        Ok(interner)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn test_str_interner() {
        let interner: Interner<str> = Interner::default();

        let key: &str = "Hello";
        assert_eq!(interner.intern(key), 0);

        let key: String = "world".into();
        assert_eq!(interner.intern(key), 1);

        let key: Box<str> = "Hello".into();
        assert_eq!(interner.intern(key), 0);

        let key: Arc<str> = "world".into();
        assert_eq!(interner.intern(key), 1);

        let key: Cow<'_, str> = "Hello world".into();
        assert_eq!(interner.intern(key), 2);
    }
}
