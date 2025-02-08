use crate::size::EstimateSize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::Deref;
use std::rc::Rc;

pub type IString = Interned<String>;
pub type StringInterner = Interner<String>;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Interned<T> {
    id: usize,
    _phantom: PhantomData<fn() -> T>,
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

pub struct Interner<T> {
    vec: Vec<Rc<T>>,
    map: HashMap<Rc<T>, usize>,
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
    fn intern(&mut self, value: T) -> usize {
        self.references += 1;

        if let Some(&id) = self.map.get(&value) {
            return id;
        }

        let id = self.vec.len();
        let rc: Rc<T> = Rc::new(value);
        self.vec.push(Rc::clone(&rc));
        self.map.insert(rc, id);
        id
    }

    fn lookup(&self, id: usize) -> Rc<T> {
        Rc::clone(&self.vec[id])
    }
}
