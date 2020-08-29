//! # rust client for lmstfy
//!
//! An api wrapper for communicating with lmstfy server

mod retrier;

pub mod api;
pub mod errors;

#[cfg(test)]
mod api_test;
