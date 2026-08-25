//! The language's rules, as pure functions over rows.
//!
//! Nothing here touches a database, a runtime, or a clock. Supersession,
//! precedence, staleness, grain and conditional relevance are decisions
//! about rows, and a decision that needs a store to be tested is a
//! decision nobody can read.
//!
//! [`latest_by`] takes the ordering column as a closure, so the same
//! rule serves any backend's ordering — Iceberg v3's
//! `_last_updated_sequence_number` today — without being rewritten.
//! While the rule lives inside a SQL `NOT EXISTS`, it has to be
//! reimplemented for every backend that carries it.

use serde_json::Value;

use crate::{Error, Result};

/// Precedence, as the collapse orders it: a human outranks an agent,
/// and both outrank a computed value (SPEC.md §5.3).
///
/// A measurement never competes here — the store admits exactly one
/// producer per measurement aspect, so its group holds one slot and the
/// rank has nothing to sort against.
pub const RANK_HUMAN: u8 = 0;
pub const RANK_AGENT: u8 = 1;
pub const RANK_FUNCTION: u8 = 2;
// The order is the ranking: lower outranks higher, and the three stay
// in this order or `serving` serves the wrong voice.
const _: () = assert!(RANK_HUMAN < RANK_AGENT && RANK_AGENT < RANK_FUNCTION);

pub fn rank_of(actor_kind: &str) -> u8 {
    match actor_kind {
        "human" => RANK_HUMAN,
        _ => RANK_AGENT,
    }
}

/// One current slot under (subject, aspect): a gloss, or a witness-bound
/// function's output. The collapse and the raw read both build on these.
#[derive(Debug, Clone)]
pub struct Slot {
    pub subject: String,
    pub aspect: String,
    /// 0 = human, 1 = agent, 2 = function.
    pub rank: u8,
    pub actor: String,
    pub witness: Option<String>,
    pub body: String,
    pub written_at: String,
    pub snapshot_id: Option<i64>,
    /// Whether the slot stands at the read's pin. A gloss always does;
    /// a function voice is the newest landing whatever its pin, and
    /// says here when that pin is an earlier one — served and marked.
    pub current: bool,
}

/// **Supersession.** Of rows sharing a key, the one written last stands;
/// the rest are history. `seq` is whatever the store orders by.
///
/// Order is not preserved — callers sort for their own purposes.
pub fn latest_by<T, K, S>(rows: Vec<T>, key: impl Fn(&T) -> K, seq: impl Fn(&T) -> S) -> Vec<T>
where
    K: std::hash::Hash + Eq,
    S: PartialOrd,
{
    let mut winners: std::collections::HashMap<K, T> = std::collections::HashMap::new();
    for row in rows {
        match winners.get(&key(&row)) {
            Some(held) if seq(held) >= seq(&row) => {}
            _ => {
                winners.insert(key(&row), row);
            }
        }
    }
    winners.into_values().collect()
}

/// **Precedence.** The slot that serves: the lowest rank in the group.
/// Ties keep the first, which is the store's own order.
pub fn serving(group: &[&Slot]) -> Option<usize> {
    group
        .iter()
        .enumerate()
        .min_by_key(|(i, s)| (s.rank, *i))
        .map(|(i, _)| i)
}

/// **The withholding band.** The detector's own band is the verdict, and
/// `red` is the crossing — whatever score produced it. The collapse
/// never re-derives a crossing from score against the witness
/// THRESHOLD: a detector may band against an authored expectation
/// instead (`rate_tolerance`, where the tolerance wins and the
/// THRESHOLD is only its fallback), and re-deriving would judge that
/// score a second time against the line the detector deliberately
/// replaced — reading green while withholding the value.
pub fn withholds(band: &str) -> bool {
    band == "red"
}

/// **Contest.** A detector crossing withholds a value only where voices
/// could actually differ: a single-speaker
/// measurement whose detector crossed would read as `contested`, and the
/// withholding would hide the body at its most interesting moment. One
/// slot cannot contest — the crossing still shows as its band, beside
/// the value.
pub fn contested(crossing: bool, voices: usize) -> bool {
    crossing && voices >= 2
}

/// **Serve and mark**: staleness never
/// suppresses a value, it shows beside it. A slot written against a
/// snapshot the table has since moved past is `stale`, and still served.
pub fn state(seen: Option<i64>, current: Option<i64>) -> &'static str {
    match (seen, current) {
        (Some(seen), Some(current)) if seen != current => "stale",
        _ => "current",
    }
}

/// The grain of a canonical subject spelling: the dataset itself, a pair
/// path, `table.column` (a composite endpoint's tuple counts as its
/// pair's grain, never a column), or a bare table.
pub fn grain_of(dataset: &str, subject: &str) -> &'static str {
    if subject == dataset {
        "dataset"
    } else if subject.contains(" -> ") || subject.contains(" <-> ") {
        "relationship"
    } else if subject.contains('.') {
        "column"
    } else {
        "table"
    }
}

/// **Grain admission**: an aspect declared `ON grain,
/// …` only accepts subjects of those grains; `None` (no clause) admits
/// all. A name that names a declared source is SOURCE grain, satisfying
/// `source` and only `source`: a table-grain aspect that accepted
/// `GLOSS entity ON erp` would put unfillable rows in the backlog.
/// `is_source` is the caller's lookup against the `sources` relation,
/// and the backlog mirrors this rule.
///
/// A source may carry its dataset's own name, and then the subject
/// spells two things. **The aspect's declaration says which is meant**
/// — either reading satisfying it is enough, so a dataset stays
/// glossable when a source shares its name and the source stays
/// addressable either way. There is no precedence between the two: the
/// read side disambiguates the same way, treating a row as source-grain
/// only when its aspect is source-grain too, never on the subject alone.
///
/// This resolves only the source/dataset collision, which the arguments
/// here can see. A source sharing a *table's* name is not resolvable
/// from a name — [`grain_of`] answers `table` for every bare word,
/// whether or not a table by that name exists — so the source reading
/// stands there, which is what keeps `GLOSS entity ON erp` out of the
/// backlog.
pub fn admit_grain(
    aspect: &str,
    grains: Option<&str>,
    dataset: &str,
    subject: &str,
    is_source: bool,
) -> Result<()> {
    let Some(declared) = grains else {
        return Ok(());
    };
    let admits = |grain: &str| declared.split(',').any(|g| g == grain);
    let effective = if is_source {
        "source"
    } else {
        grain_of(dataset, subject)
    };
    if admits(effective) {
        return Ok(());
    }
    let ambiguous = is_source && subject == dataset;
    if ambiguous && admits("dataset") {
        return Ok(());
    }
    Err(Error::GrainRefused {
        aspect: aspect.into(),
        subject: subject.into(),
        grain: if ambiguous {
            "source or dataset"
        } else {
            effective
        },
        declared: declared.to_uppercase().replace(',', ", "),
    })
}

/// **Conditional relevance**: a conditioned aspect is
/// owed on a subject only while the named sibling aspect's winning slot
/// carries the declared value. **Absence is decisive** — no sibling slot
/// spoken means nothing is owed yet, rather than everything.
pub fn condition_holds(condition_value: &str, sibling_value: Option<&str>) -> bool {
    sibling_value == Some(condition_value)
}

/// The winning value of a sibling aspect per subject, for
/// [`condition_holds`] to read: lowest rank wins, and a body with no
/// `/value` string says nothing.
pub fn sibling_winners(slots: &[Slot]) -> std::collections::HashMap<String, String> {
    let mut winners: std::collections::HashMap<String, (u8, String)> = Default::default();
    for slot in slots {
        let Some(value) = serde_json::from_str::<Value>(&slot.body)
            .ok()
            .and_then(|b| {
                b.pointer("/value")
                    .and_then(|v| v.as_str().map(String::from))
            })
        else {
            continue;
        };
        match winners.get(&slot.subject) {
            Some((rank, _)) if *rank <= slot.rank => {}
            _ => {
                winners.insert(slot.subject.clone(), (slot.rank, value));
            }
        }
    }
    winners.into_iter().map(|(s, (_, v))| (s, v)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(subject: &str, rank: u8, body: &str) -> Slot {
        Slot {
            subject: subject.into(),
            aspect: "role".into(),
            rank,
            actor: "a".into(),
            witness: None,
            body: body.into(),
            written_at: "2026-08-17".into(),
            snapshot_id: None,
            current: true,
        }
    }

    #[test]
    fn the_last_write_supersedes_its_key_and_no_other() {
        let rows = vec![
            ("a", "human", 1),
            ("a", "human", 7),
            ("a", "agent", 3),
            ("b", "human", 2),
        ];
        let mut kept = latest_by(
            rows,
            |r: &(&str, &str, i64)| (r.0.to_string(), r.1.to_string()),
            |r| r.2,
        );
        kept.sort();
        assert_eq!(
            kept,
            vec![("a", "agent", 3), ("a", "human", 7), ("b", "human", 2)]
        );
    }

    #[test]
    fn an_equal_sequence_keeps_the_row_already_held() {
        // Ties cannot happen while ids autoincrement, but they can once
        // ordering is a commit sequence number shared by a batch — so the
        // rule is stated rather than left to chance.
        let kept = latest_by(
            vec![("k", 5, "first"), ("k", 5, "second")],
            |r| r.0,
            |r| r.1,
        );
        assert_eq!(kept, vec![("k", 5, "first")]);
    }

    #[test]
    fn a_human_outranks_an_agent_and_both_outrank_a_function() {
        assert_eq!(rank_of("human"), RANK_HUMAN);
        assert_eq!(rank_of("agent"), RANK_AGENT);
        let (h, a) = (slot("t.c", RANK_HUMAN, "{}"), slot("t.c", RANK_AGENT, "{}"));
        assert_eq!(serving(&[&a, &h]), Some(1));
        assert_eq!(serving(&[&h, &a]), Some(0));
        assert_eq!(serving(&[]), None);
    }

    #[test]
    fn one_voice_cannot_contest_itself() {
        assert!(
            !contested(true, 1),
            "a lone measurement shows its band, not a withheld body"
        );
        assert!(contested(true, 2));
        assert!(!contested(false, 5));
    }

    #[test]
    fn staleness_marks_and_never_suppresses() {
        assert_eq!(state(Some(1), Some(2)), "stale");
        assert_eq!(state(Some(2), Some(2)), "current");
        // Nothing to compare against is not evidence of staleness.
        assert_eq!(state(None, Some(2)), "current");
        assert_eq!(state(Some(1), None), "current");
    }

    #[test]
    fn grain_reads_the_subject_spelling() {
        assert_eq!(grain_of("fin", "fin"), "dataset");
        assert_eq!(grain_of("fin", "orders"), "table");
        assert_eq!(grain_of("fin", "orders.amount"), "column");
        assert_eq!(grain_of("fin", "orders.id -> customers.id"), "relationship");
        // A composite endpoint is its pair's grain, never a column.
        assert_eq!(grain_of("fin", "t.(a, b) -> u.(a, b)"), "relationship");
    }

    #[test]
    fn a_bare_name_is_source_grain_only_when_it_names_a_source() {
        assert!(admit_grain("entity", Some("source"), "fin", "erp", true).is_ok());
        assert!(admit_grain("entity", Some("source"), "fin", "erp", false).is_err());
        assert!(admit_grain("role", Some("table"), "fin", "erp", true).is_err());
        // No ON clause admits everything.
        assert!(admit_grain("free", None, "fin", "anything.at.all", false).is_ok());
    }

    #[test]
    fn absence_of_the_sibling_is_decisive() {
        assert!(condition_holds("measure", Some("measure")));
        assert!(!condition_holds("measure", Some("dimension")));
        assert!(
            !condition_holds("measure", None),
            "nothing spoken, nothing owed"
        );
    }

    #[test]
    fn the_sibling_winner_is_the_human_and_bodies_without_a_value_say_nothing() {
        let slots = vec![
            slot("t.c", RANK_AGENT, r#"{"value":"dimension"}"#),
            slot("t.c", RANK_HUMAN, r#"{"value":"measure"}"#),
            slot("t.d", RANK_AGENT, r#"{"note":"no value here"}"#),
        ];
        let winners = sibling_winners(&slots);
        assert_eq!(winners.get("t.c").map(String::as_str), Some("measure"));
        assert_eq!(winners.get("t.d"), None);
    }
}
