//! A question belongs to one dataset, and so does the ruling that
//! closes it.
//!
//! A workspace holds many datasets and a subject name is unique only
//! within one — two datasets may each hold `orders`, and SPEC.md §5.3
//! lets the same QUERY aspect be glossed on a table in each. The store's
//! own collapse keys a dataset-scoped row on (dataset, subject, aspect,
//! actor kind); the reads that derive questions rebuild that rule in
//! SQL, and while they omitted `dataset` two things went wrong at once:
//! one dataset's assumptions never surfaced at all, and answering a
//! question in one dataset closed it in the other.
//!
//! Carrying `dataset` is half of it. The other half is being able to say
//! which one: `open_questions` and `ruling_entries` are workspace-wide
//! and leave the narrowing to their caller, and a caller writing SQL had
//! no way to name the session's own dataset. `current_dataset` is that
//! name, and `owed` is the first read to use it.

use std::sync::Arc;

use glossql_catalog::Lake;
use glossql_glossary::{Actor, ActorKind, Store};
use glossql_session::{NoRuntime, Outcome, Plane};

fn agent() -> Actor {
    Actor {
        kind: ActorKind::Agent,
        id: "a".into(),
    }
}

fn human() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: "h".into(),
    }
}

/// Every row as `dataset|subject|aspect|key`, sorted — the identity of
/// a question, and nothing else.
fn keys(outcomes: &[Outcome]) -> Vec<String> {
    let Outcome::Rows(batches) = outcomes.last().unwrap() else {
        panic!("expected rows")
    };
    let mut out: Vec<String> = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .flat_map(|b| {
            (0..b.num_rows()).map(move |row| {
                (0..b.num_columns())
                    .map(|c| {
                        datafusion::arrow::util::display::array_value_to_string(b.column(c), row)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    out.sort();
    out
}

const QUESTIONS: &str = "SELECT dataset, subject, aspect, key FROM open_questions;";

#[tokio::test(flavor = "multi_thread")]
async fn a_ruling_closes_its_own_dataset_s_question_and_no_other() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let plane = Plane::new(Store::open(lake).await.unwrap(), Arc::new(NoRuntime));

    // The shipped ruling aspect, so this exercises the schema a real
    // workspace enforces rather than a permissive hand-rolled copy.
    let kit = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../scripts/functions/kpi_kit.glossql"),
    )
    .unwrap();
    let start = kit.find("DECLARE ASPECT ruling").expect("the kit ships it");
    let len = kit[start..].find("AS FACT;").expect("it closes") + "AS FACT;".len();
    let ruling_aspect = &kit[start..start + len];
    // And the shipped cube aspect: `owed` reads the cube's fact rows
    // for what a grounding wants, and the rows are computed under it.
    let start = kit.find("DECLARE ASPECT cube").expect("the kit ships it");
    let len =
        kit[start..].find("AS FACT ON DATASET;").expect("it closes") + "AS FACT ON DATASET;".len();
    let cube_aspect = &kit[start..start + len];

    // Two datasets, each holding a table called `orders`, each carrying
    // the same aspect on it under the same assumption key.
    for dataset in ["fin", "ops"] {
        plane
            .execute(
                agent(),
                None,
                &format!(
                    r#"DECLARE DATASET {dataset} SET (purpose: 'question scope');
                       USE {dataset};
                       {ruling_aspect}
                       {cube_aspect}
                       DECLARE ASPECT revenue WITH $${{"title": "Revenue"}}$$ AS QUERY;
                       GLOSS revenue ON orders AS $${{"sql": "SELECT 1 AS value",
                         "assumptions": [{{"dimension": "definition", "key": "net-of-returns",
                           "assumption": "net of returns", "basis": "judgment",
                           "confidence": 0.6}}]}}$$;"#
                ),
            )
            .await
            .unwrap();
    }

    // Two datasets disclosed the claim, so two questions stand. Keyed
    // without the dataset, the later gloss superseded the earlier one
    // and `fin` never appeared.
    let both = vec![
        "fin|orders|revenue|net-of-returns".to_string(),
        "ops|orders|revenue|net-of-returns".to_string(),
    ];
    assert_eq!(
        keys(
            &plane
                .execute(agent(), Some("fin"), QUESTIONS)
                .await
                .unwrap()
        ),
        both
    );

    // The human answers in `fin`.
    plane
        .execute(
            human(),
            Some("fin"),
            r#"USE fin;
               GLOSS ruling ON orders AS $${"rulings": [{"aspect": "revenue",
                 "key": "net-of-returns", "stance": "confirmed",
                 "dimension": "definition", "assumption": "net of returns"}]}$$;"#,
        )
        .await
        .unwrap();

    // `ops` still owes its own answer: the same claim on a same-named
    // table in another dataset is a different claim.
    assert_eq!(
        keys(
            &plane
                .execute(agent(), Some("ops"), QUESTIONS)
                .await
                .unwrap()
        ),
        vec!["ops|orders|revenue|net-of-returns".to_string()],
        "the ruling in `fin` reached across into `ops`"
    );

    // And the debt that ruling created is `fin`'s. `owed` narrows itself
    // rather than leaving it to the caller, so the same statement run in
    // two sessions answers for two datasets.
    const OWED: &str = "SELECT kind, subject FROM owed;";
    assert_eq!(
        keys(&plane.execute(agent(), Some("fin"), OWED).await.unwrap()),
        vec!["fold-in|revenue".to_string()],
        "the fold-in `fin` owes"
    );
    assert_eq!(
        keys(&plane.execute(agent(), Some("ops"), OWED).await.unwrap()),
        Vec::<String>::new(),
        "`ops` was never ruled on and owes no fold-in"
    );
}

/// `current_dataset` — one row while a dataset is bound, no rows when
/// none is.
///
/// It exists because SQL inside a read cannot reach the session state
/// the way a door written in Rust can, so every read over the
/// workspace-wide relations either narrowed itself by joining this or
/// answered for the whole workspace. The empty case is the load-bearing
/// one: a read that joins it inherits "nothing bound, nothing owed"
/// without testing for it, where a refusal would have to be handled by
/// every read built on top.
#[tokio::test(flavor = "multi_thread")]
async fn the_bound_dataset_is_readable_as_a_relation() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(
        &dir.path().join("catalog.db"),
        &dir.path().join("warehouse"),
    )
    .await
    .unwrap();
    let plane = Plane::new(Store::open(lake).await.unwrap(), Arc::new(NoRuntime));

    plane
        .execute(
            agent(),
            None,
            "DECLARE DATASET fin SET (purpose: 'the bound dataset');
             DECLARE DATASET ops SET (purpose: 'the other one');",
        )
        .await
        .unwrap();

    const READ: &str = "SELECT dataset FROM current_dataset;";
    for bound in ["fin", "ops"] {
        assert_eq!(
            keys(&plane.execute(agent(), Some(bound), READ).await.unwrap()),
            vec![bound.to_string()],
            "the session bound to `{bound}` reads its own name"
        );
    }

    // No `USE`, no rows — not a null and not a refusal.
    assert_eq!(
        keys(&plane.execute(agent(), None, READ).await.unwrap()),
        Vec::<String>::new(),
        "an unbound session names no dataset"
    );

    // It composes as a relation, which is the whole point: the join is
    // how a read narrows itself.
    assert_eq!(
        keys(
            &plane
                .execute(
                    agent(),
                    Some("fin"),
                    "SELECT s.name FROM datasets s JOIN current_dataset d ON d.dataset = s.name;",
                )
                .await
                .unwrap()
        ),
        vec!["fin".to_string()],
        "joined against the workspace's datasets, it selects the bound one"
    );

    // A CTE of the same name wins, which is the opposite of how a
    // shipped read behaves — those are reserved and shadow a CTE.
    // `current_dataset` is a compute door rather than a `.sql` file in
    // the library, and the pre-pass declines a shadowed factor before
    // it computes anything, so SQL's own precedence stands. Checked
    // rather than reasoned about: the two paths part company in
    // `prepass::shadowed`, which asks the library and gets `None`.
    assert_eq!(
        keys(
            &plane
                .execute(
                    agent(),
                    Some("fin"),
                    "WITH current_dataset AS (SELECT 'ops' AS dataset)                      SELECT dataset FROM current_dataset;",
                )
                .await
                .unwrap()
        ),
        vec!["ops".to_string()],
        "a CTE of that name is the author's, not ours"
    );
}
