use crate::size::EstimateSize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::ops::Deref;
use std::rc::Rc;

pub type IString<'a> = Interned<'a, String>;
pub type StringInterner = Interner<String>;

pub struct Interned<'a, T> {
    interner: &'a Interner<T>,
    id: usize,
}

impl<T: Eq + Hash> PartialEq for Interned<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.lookup().deref() == other.lookup().deref()
    }
}

impl<T: Eq + Hash> Eq for Interned<'_, T> {}

impl<T: Eq + Hash> Hash for Interned<'_, T> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.lookup().deref().hash(state)
    }
}

impl<T: Eq + Hash> PartialEq<T> for Interned<'_, T> {
    fn eq(&self, other: &T) -> bool {
        self.lookup().deref() == other
    }
}

impl<T: Eq + Hash + Debug> Debug for Interned<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.lookup().deref().fmt(f)
    }
}

impl<T> EstimateSize for Interned<'_, T> {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl<'a, T: Eq + Hash> Interned<'a, T> {
    pub fn from(interner: &'a Interner<T>, value: T) -> Self {
        interner.intern(value)
    }
}

impl<T: Eq + Hash> Interned<'_, T> {
    pub fn lookup(&self) -> Rc<T> {
        self.interner.lookup(self.id)
    }
}

pub struct Interner<T> {
    inner: RefCell<InternerImpl<T>>,
}

impl<T> Default for Interner<T> {
    fn default() -> Self {
        Self {
            inner: RefCell::new(InternerImpl::default()),
        }
    }
}

impl<T: EstimateSize> EstimateSize for Interner<T> {
    fn allocated_bytes(&self) -> usize {
        self.inner.borrow().allocated_bytes()
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
        self.inner.borrow().len()
    }

    fn references(&self) -> usize {
        self.inner.borrow().references()
    }
}

impl<T: Eq + Hash> Interner<T> {
    fn intern(&self, value: T) -> Interned<'_, T> {
        let id = self.inner.borrow_mut().intern(value);
        Interned { interner: self, id }
    }

    fn lookup(&self, i: usize) -> Rc<T> {
        self.inner.borrow().lookup(i)
    }
}

struct InternerImpl<T> {
    vec: Vec<Rc<T>>,
    map: HashMap<Rc<T>, usize>,
    references: usize,
}

impl<T> Default for InternerImpl<T> {
    fn default() -> Self {
        Self {
            vec: Vec::new(),
            map: HashMap::new(),
            references: 0,
        }
    }
}

impl<T: EstimateSize> EstimateSize for InternerImpl<T> {
    fn allocated_bytes(&self) -> usize {
        self.vec.iter().map(|x| x.estimated_bytes()).sum::<usize>()
            + self.map.capacity() * size_of::<Rc<T>>()
    }
}

impl<T> InternerImpl<T> {
    fn len(&self) -> usize {
        self.vec.len()
    }

    fn references(&self) -> usize {
        self.references
    }
}

impl<T: Eq + Hash> InternerImpl<T> {
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
