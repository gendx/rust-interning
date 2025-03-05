use std::mem::size_of;
use uuid::Uuid;

pub trait StackSize {
    fn stack_bytes(&self) -> usize;
}

impl<T: Sized> StackSize for T {
    fn stack_bytes(&self) -> usize {
        size_of::<Self>()
    }
}

pub trait EstimateSize: StackSize {
    fn allocated_bytes(&self) -> usize;

    fn estimated_bytes(&self) -> usize {
        self.stack_bytes() + self.allocated_bytes()
    }
}

impl EstimateSize for i32 {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl StackSize for str {
    fn stack_bytes(&self) -> usize {
        0
    }
}

impl EstimateSize for str {
    fn allocated_bytes(&self) -> usize {
        self.len()
    }
}

impl EstimateSize for String {
    fn allocated_bytes(&self) -> usize {
        self.len()
    }
}

impl EstimateSize for Uuid {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl<T: EstimateSize> EstimateSize for Option<T> {
    fn allocated_bytes(&self) -> usize {
        self.as_ref().map_or(0, |x| x.allocated_bytes())
    }
}

impl<T: EstimateSize> EstimateSize for Vec<T> {
    fn allocated_bytes(&self) -> usize {
        self.iter().map(|x| x.estimated_bytes()).sum()
    }
}

impl<T: EstimateSize> EstimateSize for Box<[T]> {
    fn allocated_bytes(&self) -> usize {
        self.iter().map(|x| x.estimated_bytes()).sum()
    }
}
