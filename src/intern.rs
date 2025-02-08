use crate::size::EstimateSize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem::size_of;
use std::ops::Deref;
use std::rc::Rc;

pub struct IString<'a> {
    interner: &'a StringInterner,
    id: usize,
}

impl PartialEq<String> for IString<'_> {
    fn eq(&self, other: &String) -> bool {
        self.lookup().deref() == other
    }
}

impl Debug for IString<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        self.lookup().deref().fmt(f)
    }
}

impl EstimateSize for IString<'_> {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl<'a> IString<'a> {
    pub fn from(interner: &'a StringInterner, value: String) -> Self {
        interner.intern(value)
    }
}

impl IString<'_> {
    pub fn lookup(&self) -> Rc<String> {
        self.interner.lookup(self.id)
    }
}

#[derive(Default)]
pub struct StringInterner {
    inner: RefCell<StringInternerImpl>,
}

impl EstimateSize for StringInterner {
    fn allocated_bytes(&self) -> usize {
        self.inner.borrow().allocated_bytes()
    }
}

impl StringInterner {
    pub fn print_summary(&self, total_bytes: usize) {
        let len = self.len();
        let references = self.references();
        let estimated_bytes = self.estimated_bytes();
        println!(
            "- [{:.02}%] String interner: {} objects | {} bytes ({:.02} bytes/object) | {} references ({:.02} refs/object)",
            estimated_bytes as f64 * 100.0 / total_bytes as f64,
            len,
            estimated_bytes,
            estimated_bytes as f64 / len as f64,
            references,
            references as f64 / len as f64,
        );
    }

    fn len(&self) -> usize {
        self.inner.borrow().len()
    }

    fn references(&self) -> usize {
        self.inner.borrow().references()
    }

    fn intern(&self, value: String) -> IString<'_> {
        let id = self.inner.borrow_mut().intern(value);
        IString { interner: self, id }
    }

    fn lookup(&self, i: usize) -> Rc<String> {
        self.inner.borrow().lookup(i)
    }
}

#[derive(Default)]
struct StringInternerImpl {
    vec: Vec<Rc<String>>,
    map: HashMap<Rc<String>, usize>,
    references: usize,
}

impl EstimateSize for StringInternerImpl {
    fn allocated_bytes(&self) -> usize {
        self.vec.iter().map(|x| x.estimated_bytes()).sum::<usize>()
            + self.map.capacity() * size_of::<Rc<String>>()
    }
}

impl StringInternerImpl {
    fn len(&self) -> usize {
        self.vec.len()
    }

    fn references(&self) -> usize {
        self.references
    }

    fn intern(&mut self, value: String) -> usize {
        self.references += 1;

        if let Some(&id) = self.map.get(&value) {
            return id;
        }

        let id = self.vec.len();
        let rc: Rc<String> = Rc::new(value);
        self.vec.push(Rc::clone(&rc));
        self.map.insert(rc, id);
        id
    }

    fn lookup(&self, id: usize) -> Rc<String> {
        Rc::clone(&self.vec[id])
    }
}
