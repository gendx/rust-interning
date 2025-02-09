use crate::size::EstimateSize;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_tuple::{Deserialize_tuple, Serialize_tuple};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Deref;
use std::rc::Rc;

pub type IString = Interned<String>;
pub type StringInterner = Interner<String>;

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct Interned<T> {
    id: u32,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Debug for Interned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_tuple("I").field(&self.id).finish()
    }
}

impl<T> PartialEq for Interned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<T> Eq for Interned<T> {}

impl<T> PartialOrd for Interned<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Interned<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<T> Hash for Interned<T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T> EstimateSize for Interned<T> {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl<T: Eq + Hash> Interned<T> {
    pub fn from(interner: &mut Interner<T>, value: T) -> Self {
        let id = interner.intern(value);
        Self {
            id,
            _phantom: PhantomData,
        }
    }

    pub fn lookup(&self, interner: &Interner<T>) -> Rc<T> {
        interner.lookup(self.id)
    }
}

pub trait EqWith<Rhs, Helper> {
    fn eq_with(&self, other: &Rhs, helper: &Helper) -> bool;
}

impl<T: Eq + Hash> EqWith<T, Interner<T>> for Interned<T> {
    fn eq_with(&self, other: &T, interner: &Interner<T>) -> bool {
        self.lookup(interner).deref() == other
    }
}

impl<T: Eq + Hash> Interned<T> {
    pub fn eq_with_more<U, Helper>(
        &self,
        other: &U,
        interner: &Interner<T>,
        helper: &Helper,
    ) -> bool
    where
        T: EqWith<U, Helper>,
    {
        self.lookup(interner).deref().eq_with(other, helper)
    }
}

#[derive(Debug)]
pub struct Interner<T> {
    vec: Vec<Rc<T>>,
    map: HashMap<Rc<T>, u32>,
    references: usize,
}

impl<T> Default for Interner<T> {
    fn default() -> Self {
        Self {
            vec: Vec::new(),
            map: HashMap::new(),
            references: 0,
        }
    }
}

impl<T: Eq + Hash> PartialEq for Interner<T> {
    fn eq(&self, other: &Self) -> bool {
        self.vec == other.vec && self.map == other.map
    }
}

impl<T: Eq + Hash> Eq for Interner<T> {}

impl<T: EstimateSize> EstimateSize for Interner<T> {
    fn allocated_bytes(&self) -> usize {
        self.vec.iter().map(|x| x.estimated_bytes()).sum::<usize>()
            + self.map.capacity() * size_of::<Rc<T>>()
    }
}

impl<T: EstimateSize> Interner<T> {
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

impl<T> Interner<T> {
    fn len(&self) -> usize {
        self.vec.len()
    }

    fn references(&self) -> usize {
        self.references
    }
}

impl<T: Eq + Hash> Interner<T> {
    fn intern(&mut self, value: T) -> u32 {
        self.references += 1;

        if let Some(&id) = self.map.get(&value) {
            return id;
        }

        self.push(value)
    }

    /// Unconditionally push a value, without validating that it's already interned.
    fn push(&mut self, value: T) -> u32 {
        let id = self.vec.len();
        assert!(id <= u32::MAX as usize);
        let id = id as u32;

        let rc: Rc<T> = Rc::new(value);
        self.vec.push(Rc::clone(&rc));
        self.map.insert(rc, id);

        id
    }

    fn lookup(&self, id: u32) -> Rc<T> {
        Rc::clone(&self.vec[id as usize])
    }
}

impl<T: Serialize> Serialize for Interner<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.vec.iter().map(|rc| rc.deref()))
    }
}

impl<'de, T> Deserialize<'de> for Interner<T>
where
    T: Eq + Hash + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(InternerVisitor::new())
    }
}

struct InternerVisitor<T> {
    _phantom: PhantomData<fn() -> Interner<T>>,
}

impl<T> InternerVisitor<T> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<'de, T> Visitor<'de> for InternerVisitor<T>
where
    T: Eq + Hash + Deserialize<'de>,
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
                vec: Vec::with_capacity(size_hint),
                map: HashMap::with_capacity(size_hint),
                references: 0,
            },
        };

        while let Some(t) = seq.next_element()? {
            interner.push(t);
        }

        Ok(interner)
    }
}
