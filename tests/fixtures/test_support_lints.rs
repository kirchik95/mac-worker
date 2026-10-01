mod owner {
    pub fn production_entry() {}

    #[cfg(feature = "test-support")]
    pub fn fixture_entry() {}

    #[cfg(feature = "lint-probe")]
    pub fn unused_production_helper() {}
}

pub use owner::production_entry;

#[cfg(feature = "test-support")]
pub mod test_support {
    pub use crate::owner::fixture_entry;
}
