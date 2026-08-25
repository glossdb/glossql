//! glossql front parser on DataFusion's parser machinery.
//!
//! [`GlossqlParser`] wraps `DFParser` — the in-tree custom-statement
//! pattern: one stock-sqlparser token stream (postgres dialect), glossql
//! heads hand-parsed with `Parser` primitives, every other statement
//! delegated and carried as [`Statement::Substrate`]. The substrate is
//! DataFusion's to plan; this crate never re-parses SQL, and the
//! `GLOSSARY()` / `ATTEST()` reads live in the substrate as ordinary table
//! functions.
//!
//! The corpus under `corpus/` is this crate's acceptance suite — the
//! standing invariant. The spelling constraints (dollar-quoted bodies,
//! string schema pointers, dollar-quoted recipe tails) exist because
//! `DFParser` tokenizes the entire input up front with stock sqlparser:
//! anything the tokenizer mangles is unrecoverable, so every byte of
//! glossql must lex as stock tokens. Bare-brace JSON dies in the
//! tokenizer (`\"` ends a double-quoted region), `#/` pointers lex
//! differently per dialect, and one alien lexeme in a foreign-dialect
//! recipe tail fails the whole script — dollar quotes carry all three
//! byte-exact, with no escaping.

// An unwrap outside a test is a panic waiting for the row that has it;
// tests are exempt (clippy.toml).
#![warn(clippy::unwrap_used)]

mod ast;
mod parser;

pub use ast::*;
pub use parser::GlossqlParser;
