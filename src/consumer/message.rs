#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub(crate) String);

impl MessageId {
    /// Creates a new [`MessageId`].
    ///
    /// This should not be called from user code; this should only be
    /// used by providers, to create a `MessageId`.  This is mostly to
    /// enforce with type-level restraints what can be used as a
    /// message id. (i.e., to force user-code to only provide
    /// message IDs that are valid for the provider, and to
    /// prevent user-code from creating message IDs that are
    /// invalid for the provider.)
    #[must_use]
    pub(crate) fn new(id: impl Into<String>) -> Self { Self(id.into()) }
}

impl AsRef<str> for MessageId {
    fn as_ref(&self) -> &str { &self.0 }
}

impl std::ops::Deref for MessageId {
    type Target = str;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct Message<T> {
    pub id: MessageId,
    pub data: T,
    pub retries: u64,
}

impl<T> Message<T> {
    pub fn new(id: MessageId, data: T, retries: u64) -> Self { Self { id, data, retries } }
}
