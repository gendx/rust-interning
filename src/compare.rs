use crate::intern::{Interned, Interner};
use std::borrow::Borrow;
use std::hash::Hash;

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
