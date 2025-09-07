#![allow(non_local_definitions)]
use std::{error::Error, fmt};

#[derive(Debug)]
pub struct AuthError {
    pub value: AuthErrorValue,
}

#[derive(Debug, thiserror::Error)]
pub enum AuthErrorValue {
    #[error("token is not correct.")]
    TokenIsNotCorrect,
    #[error("no token found.")]
    NoTokenFound,
    #[error("invalid token format.")]
    InvalidTokenFormat,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl Error for AuthError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.value.source()
    }
}
