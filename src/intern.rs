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
use std::sync::atomic::{self, AtomicUsize};

pub type IString = Interned<str, Box<str>>;
pub type StringInterner = Interner<str, Box<str>>;

pub struct Interned<T: ?Sized, Storage = T> {
    id: u32,
    _phantom: PhantomData<fn() -> (*const T, *const Storage)>,
}

impl<T: ?Sized, Storage> Debug for Interned<T, Storage> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<T: ?Sized, Storage> PartialEq for Interned<T, Storage> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T: ?Sized, Storage> Eq for Interned<T, Storage> {}

impl<T: ?Sized, Storage> PartialOrd for Interned<T, Storage> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: ?Sized, Storage> Ord for Interned<T, Storage> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<T: ?Sized, Storage> Hash for Interned<T, Storage> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T: ?Sized, Storage> EstimateSize for Interned<T, Storage> {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage> {
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

impl<T: ?Sized, Storage> Interned<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    pub fn from(interner: &Interner<T, Storage>, value: impl Borrow<T> + Into<Storage>) -> Self {
        let id = interner.intern(value);
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage>
where
    Storage: Clone,
{
    #[expect(dead_code)]
    pub fn lookup(&self, interner: &Interner<T, Storage>) -> Storage {
        interner.lookup(self.id)
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage>
where
    Storage: Borrow<T>,
{
    pub fn lookup_ref<'a>(&self, interner: &'a Interner<T, Storage>) -> &'a T {
        interner.lookup_ref(self.id)
    }
}

pub trait EqWith<Rhs: ?Sized, Helper: ?Sized> {
    fn eq_with(&self, other: &Rhs, helper: &Helper) -> bool;
}

impl<T: ?Sized, Storage> EqWith<T, Interner<T, Storage>> for Interned<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    fn eq_with(&self, other: &T, interner: &Interner<T, Storage>) -> bool {
        self.lookup_ref(interner) == other
    }
}

impl<T: ?Sized, Storage> Interned<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    pub fn eq_with_more<U, Helper>(
        &self,
        other: &U,
        interner: &Interner<T, Storage>,
        helper: &Helper,
    ) -> bool
    where
        T: EqWith<U, Helper>,
    {
        self.lookup_ref(interner).eq_with(other, helper)
    }
}

impl<T: ?Sized, Storage> Serialize for Interned<T, Storage> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.id)
    }
}

impl<'de, T: ?Sized, Storage> Deserialize<'de> for Interned<T, Storage> {
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

pub struct Interner<T: ?Sized, Storage = T> {
    vec: AppendVec<Storage>,
    map: DashTable<u32>,
    hasher: DefaultHashBuilder,
    references: AtomicUsize,
    _phantom: PhantomData<fn() -> *const T>,
}

impl<T: ?Sized, Storage> Default for Interner<T, Storage> {
    fn default() -> Self {
        Self {
            vec: AppendVec::new(),
            map: DashTable::new(),
            hasher: DefaultHashBuilder::default(),
            references: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }
}

impl<T: ?Sized, Storage> Debug for Interner<T, Storage>
where
    T: Debug,
    Storage: Borrow<T>,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_list()
            .entries(self.vec.iter().map(|x| x.borrow()))
            .finish()
    }
}

impl<T: ?Sized, Storage> PartialEq for Interner<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    fn eq(&self, other: &Self) -> bool {
        self.vec
            .iter()
            .map(|x| x.borrow())
            .eq(other.vec.iter().map(|x| x.borrow()))
    }
}

impl<T: ?Sized, Storage> Eq for Interner<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
}

impl<T: ?Sized, Storage> EstimateSize for Interner<T, Storage>
where
    Storage: EstimateSize,
{
    fn allocated_bytes(&self) -> usize {
        self.vec.iter().map(|x| x.estimated_bytes()).sum::<usize>()
            + self.vec.len() * size_of::<u32>()
    }
}

impl<T: ?Sized, Storage> Interner<T, Storage>
where
    Storage: EstimateSize,
{
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

impl<T: ?Sized, Storage> Interner<T, Storage> {
    fn len(&self) -> usize {
        self.vec.len()
    }

    fn references(&self) -> usize {
        self.references.load(atomic::Ordering::Relaxed)
    }
}

impl<T: ?Sized, Storage> Interner<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    fn intern(&self, value: impl Borrow<T> + Into<Storage>) -> u32 {
        self.references.fetch_add(1, atomic::Ordering::Relaxed);

        let hash = self.hasher.hash_one(value.borrow());
        *self
            .map
            .entry(
                hash,
                |&i| self.vec[i as usize].borrow() == value.borrow(),
                |&i| self.hasher.hash_one(self.vec[i as usize].borrow()),
            )
            .or_insert_with(|| {
                let x: Storage = value.into();
                let id = self.vec.push(x);
                assert!(id <= u32::MAX as usize);
                id as u32
            })
            .get()
    }

    /// Unconditionally push a value, without validating that it's already interned.
    fn push(&mut self, value: Storage) -> u32 {
        let hash = self.hasher.hash_one(value.borrow());

        let id = self.vec.push_mut(value);
        assert!(id <= u32::MAX as usize);
        let id = id as u32;

        self.map.insert_unique(hash, id, |&i| {
            self.hasher.hash_one(self.vec[i as usize].borrow())
        });

        id
    }
}

impl<T: ?Sized, Storage> Interner<T, Storage>
where
    Storage: Clone,
{
    fn lookup(&self, id: u32) -> Storage {
        self.vec[id as usize].clone()
    }
}

impl<T: ?Sized, Storage> Interner<T, Storage>
where
    Storage: Borrow<T>,
{
    fn lookup_ref(&self, id: u32) -> &T {
        self.vec[id as usize].borrow()
    }
}

impl<T: ?Sized, Storage> Serialize for Interner<T, Storage>
where
    T: Serialize,
    Storage: Borrow<T>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.vec.iter().map(|x| x.borrow()))
    }
}

impl<'de, T: ?Sized, Storage> Deserialize<'de> for Interner<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(InternerVisitor::new())
    }
}

struct InternerVisitor<T: ?Sized, Storage> {
    _phantom: PhantomData<fn() -> Interner<T, Storage>>,
}

impl<T: ?Sized, Storage> InternerVisitor<T, Storage> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<'de, T: ?Sized, Storage> Visitor<'de> for InternerVisitor<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T> + Deserialize<'de>,
{
    type Value = Interner<T, Storage>;

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
                _phantom: PhantomData,
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
        let interner: Interner<str, Box<str>> = Interner::default();

        let key: &str = "Hello";
        assert_eq!(interner.intern(key), 0);

        let key: String = "world".into();
        assert_eq!(interner.intern(key), 1);

        let key: Box<str> = "Hello".into();
        assert_eq!(interner.intern(key), 0);

        let key: Box<str> = "world".into();
        assert_eq!(interner.intern(key), 1);

        let key: Cow<'_, str> = "Hello world".into();
        assert_eq!(interner.intern(key), 2);
    }
}
