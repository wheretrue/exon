use noodles::core::Region;

/// A region object store extension.
pub(crate) struct RegionObjectStoreExtension {
    pub(crate) region: Region,
}

impl From<&Region> for RegionObjectStoreExtension {
    fn from(region: &Region) -> Self {
        Self {
            region: region.clone(),
        }
    }
}

impl RegionObjectStoreExtension {
    pub(crate) fn region_name(&self) -> String {
        self.region.to_string()
    }
}
