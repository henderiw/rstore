use core_apim::APIError;
use sqlx::Error as SqlxError;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct DBError(pub SqlxError); // 🔹 Newtype wrapper

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for DBError {}

impl From<DBError> for APIError {
    fn from(err: DBError) -> Self {
        match err.0 {
            // 🔹 Extract sqlx::Error from DbError
            sqlx::Error::RowNotFound => APIError::NotFound("Not found".to_string()),
            sqlx::Error::Database(db_err) => {
                if db_err.is_foreign_key_violation() {
                    APIError::Conflict("Foreign key violation".to_string())
                } else if db_err.is_unique_violation() {
                    APIError::AlreadyExists("Resource already exists".to_string())
                } else {
                    APIError::InternalServerError(format!("Database error: {}", db_err))
                }
            }
            sqlx::Error::PoolTimedOut => {
                APIError::InternalServerError("Database pool timeout".to_string())
            }
            sqlx::Error::Io(io_err) => {
                APIError::InternalServerError(format!("I/O error: {}", io_err))
            }
            _ => APIError::InternalServerError(format!("Unexpected database error: {}", err.0)),
        }
    }
}

/*
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
*/
