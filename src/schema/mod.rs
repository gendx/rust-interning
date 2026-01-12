pub mod optimized;
pub mod source;

use get_size2::GetSize;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Uuid(uuid::Uuid);

impl GetSize for Uuid {
    // There is nothing on the heap, so the default implementation works out of the box.
}
