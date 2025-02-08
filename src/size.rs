use std::mem::size_of;

pub trait EstimateSize: Sized {
    fn allocated_bytes(&self) -> usize;

    fn estimated_bytes(&self) -> usize {
        size_of::<Self>() + self.allocated_bytes()
    }
}

impl EstimateSize for i32 {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

impl EstimateSize for String {
    fn allocated_bytes(&self) -> usize {
        self.len()
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
