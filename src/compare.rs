use blazinterner::{Arena, ArenaStr, Interned, InternedStr};
use std::borrow::Borrow;
use std::hash::Hash;

pub trait EqWith<Rhs: ?Sized, Helper: ?Sized> {
    fn eq_with(&self, other: &Rhs, helper: &Helper) -> bool;
}

impl<T: ?Sized, Storage> EqWith<T, Arena<T, Storage>> for Interned<T, Storage>
where
    T: Eq + Hash,
    Storage: Borrow<T>,
{
    fn eq_with(&self, other: &T, arena: &Arena<T, Storage>) -> bool {
        arena.lookup_ref(*self) == other
    }
}

impl EqWith<str, ArenaStr> for InternedStr {
    fn eq_with(&self, other: &str, arena: &ArenaStr) -> bool {
        arena.lookup(*self) == other
    }
}
