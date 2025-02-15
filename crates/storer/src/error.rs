use std::error::Error;

#[derive(Debug)]
pub struct NotFoundError {
    pub key: String,
}

impl std::fmt::Display for NotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key {} not found", self.key)
    }
}

impl Error for NotFoundError {}

#[derive(Debug)]
pub struct AlreadyExists {
    pub key: String,
}

impl std::fmt::Display for AlreadyExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key {} already exists", self.key)
    }
}

impl Error for AlreadyExists {}
