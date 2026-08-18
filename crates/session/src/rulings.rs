//! Landing a human's ruling.
//!
//! A ruling is the human's standing judgment on one disclosed claim,
//! named by the agent's declared `key`. It lives in the human's own
//! `ruling` slot on the subject and carries the judgment alone — never
//! a copy of the agent's grounding (ruled 2026-08-14: the frozen copy
//! outranked every later correction).
//!
//! This lives here, below every door, because more than one door can
//! carry one. The MCP round asks a question mid-call and lands the
//! answer; the docket answers the same question later, when a person
//! comes back to a page rather than to a form. Both witness the human
//! themselves and neither takes the agent's word for it — that is the
//! whole provenance argument, and it holds for any door that keeps it.
//!
//! Two levels of history, and they differ on purpose:
//!
//!   * the STORE supersedes — every ruling write is a new glossary row
//!     and the newest human writing is what stands;
//!   * the SLOT overwrites — a re-ruling on the same `(aspect, key)`
//!     replaces its entry, because the slot is the standing judgment
//!     and not a transcript.
//!
//! So two rulings can only ever contradict across ASPECTS: the same
//! key confirmed on one and corrected on another, which is legitimate
//! (two groundings may genuinely differ) and which `ruling_conflicts`
//! reports. No third ruling settles it and none is needed — re-ruling
//! either side overwrites that entry, and once the stances agree the
//! pair stops matching.

use datafusion::arrow::array::{Array, StringArray};

use crate::session::{Outcome, Session};

/// One judgment, ready to land. `key` is the claim's identity and the
/// only thing the record joins on; `assumption` is the prose the human
/// actually read, kept as a display snapshot and never compared.
pub struct Ruling<'a> {
    pub subject: &'a str,
    pub aspect: &'a str,
    pub dimension: &'a str,
    pub key: &'a str,
    pub assumption: &'a str,
    /// `confirmed`, `corrected`, or `unclear` — the last takes no
    /// position on the claim: the human could not tell what was being
    /// asked, and the agent owes a reformulation under a NEW key (the
    /// entry holds this key closed; a clearer assumption with its own
    /// key derives a fresh question on its own).
    pub stance: &'a str,
    /// The human's own words, when they corrected something — or, on
    /// `unclear`, what confused them.
    pub note: Option<String>,
}

/// Path subjects only: 1–3 identifier segments (`fin`, `orders`,
/// `orders.amount`). The composed statement is dollar-quoted, so the
/// subject and aspect are the only spliced identifiers and they are
/// gated here.
pub fn ident_path(s: &str, max_segments: usize) -> bool {
    let segments: Vec<&str> = s.split('.').collect();
    !segments.is_empty()
        && segments.len() <= max_segments
        && segments.iter().all(|seg| {
            !seg.is_empty()
                && seg
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
}

/// The first row's `body` column, if the read returned one.
fn first_body(outcomes: &[Outcome]) -> Option<String> {
    for outcome in outcomes {
        let Outcome::Rows(batches) = outcome else {
            continue;
        };
        for batch in batches {
            let Some(column) = batch.column_by_name("body") else {
                continue;
            };
            let Some(text) = column.as_any().downcast_ref::<StringArray>() else {
                continue;
            };
            if text.len() > 0 && !text.is_null(0) {
                return Some(text.value(0).to_string());
            }
        }
    }
    None
}

/// Land the ruling on the human's own channel. `human` must already be
/// a session for the human actor, bound to the dataset — the caller
/// owns who is speaking, this owns what is said.
pub async fn land(human: &Session, ruling: Ruling<'_>) -> Result<String, String> {
    let Ruling {
        subject,
        aspect,
        dimension,
        key,
        assumption,
        stance,
        note,
    } = ruling;
    if !ident_path(subject, 3) {
        return Err(format!(
            "`{subject}` is not a path subject (identifier segments, dots between)"
        ));
    }
    if !ident_path(aspect, 1) {
        return Err(format!("`{aspect}` is not an aspect name"));
    }
    let read = format!(
        "SELECT body FROM glossary \
         WHERE subject = '{subject}' AND aspect = 'ruling' AND actor_kind = 'human' \
         ORDER BY written_at DESC LIMIT 1"
    );
    let standing = human
        .execute(&read)
        .await
        .map_err(|e| format!("the ruling slot read failed: {e}"))?;
    let mut body = first_body(&standing)
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .unwrap_or_else(|| serde_json::json!({ "rulings": [] }));
    let Some(rulings) = body.get_mut("rulings").and_then(|r| r.as_array_mut()) else {
        return Err("the standing ruling slot is not a ruling body".into());
    };
    rulings.retain(|r| !(r["aspect"] == aspect && r["key"] == key));
    let mut entry = serde_json::json!({
        "aspect": aspect,
        "dimension": dimension,
        "key": key,
        "assumption": assumption,
        "stance": stance,
    });
    if let Some(note) = note {
        entry["note"] = serde_json::json!(note);
    }
    rulings.push(entry);
    let body = body.to_string();
    // After a `$$` the rest would parse as further statements, and this
    // writes exactly one gloss.
    if body.contains("$$") {
        return Err("a body carrying `$$` cannot ride the dollar-quoted statement".into());
    }
    // The slot is `ruling` — always. `aspect` names what was ruled and
    // rides inside the body; writing to it instead would push a ruling
    // list at a grounding, which the standard grounding schema rightly
    // refuses.
    human
        .execute(&format!("GLOSS ruling ON {subject} AS $${body}$$;"))
        .await
        .map(|_| {
            if stance == "unclear" {
                format!(
                    "ruled (unclear) on `{key}` — the question missed: \
                     reformulate the assumption in `{aspect}` under a new \
                     key and re-record; the clearer wording derives its \
                     own question"
                )
            } else {
                format!(
                    "ruled ({stance}) on `{key}` — the fold-in is yours: \
                     re-record `{aspect}` carrying that key, citing the ruling"
                )
            }
        })
        .map_err(|e| e.to_string())
}
