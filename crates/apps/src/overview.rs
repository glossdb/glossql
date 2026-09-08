//! What the workspace root and a dataset's page read: a few counts and
//! lists over the record's own relations, rendered on the server.
//! Neither page is an app — no frames, no params, one visit — so the
//! rows come through the same channel a frame would use and land in
//! the template as JSON.

use std::collections::HashMap;

use datafusion::common::{ParamValues, ScalarValue};
use futures::StreamExt;
use glossql_glossary::{Actor, ActorKind};
use serde_json::Value;

use crate::AppDoor;

/// The datasets and their purpose. Workspace-scoped: read unbound.
pub(crate) const DATASETS: &str = "SELECT name, \
       arrow_cast(coalesce(json_get_str(settings, 'purpose'), ''), 'Utf8') AS purpose \
     FROM datasets ORDER BY name";

/// A dataset at a glance: what has landed, what is served, what waits
/// on a person. Bound to the dataset; `current_dataset` narrows the
/// workspace-wide relations.
pub(crate) const COUNTS: &str = "WITH latest AS ( \
       SELECT i.table_name, max(i.imported_at) AS imported_at \
       FROM imports i JOIN current_dataset d ON d.dataset = i.dataset \
       GROUP BY i.table_name) \
     SELECT \
       (SELECT count(*) FROM latest) AS tables, \
       (SELECT coalesce(sum(CAST(i.landed_rows AS BIGINT)), 0) \
        FROM latest l JOIN imports i \
          ON i.table_name = l.table_name AND i.imported_at = l.imported_at \
        JOIN current_dataset d ON d.dataset = i.dataset) AS rows, \
       (SELECT arrow_cast(coalesce(max(imported_at), ''), 'Utf8') FROM latest) AS landed, \
       (SELECT count(*) FROM metric_surfaces WHERE grounded AND stopped = '') AS served, \
       (SELECT count(*) FROM metric_surfaces WHERE stopped <> '') AS stopped, \
       (SELECT count(*) FROM open_questions q \
        JOIN current_dataset d ON d.dataset = q.dataset) AS open";

/// The landed tables of the bound dataset: the newest landing per
/// table is the table — a re-landing drops the old one first — with
/// its rows, the cells its casts nulled, and when. Column counts come
/// from the engine's own schema surface, joined by the landed name so
/// the format's metadata tables beside each (`<table>$history` and
/// kin) never count.
pub(crate) const TABLES: &str = "WITH latest AS ( \
       SELECT i.table_name, max(i.imported_at) AS imported_at \
       FROM imports i JOIN current_dataset d ON d.dataset = i.dataset \
       GROUP BY i.table_name), \
     nulled AS ( \
       SELECT i.table_name, i.imported_at, \
              sum(json_get_int(json_get(json_get(i.cast_failures, 'checked'), c.i), 'failed')) AS nulled \
       FROM imports i JOIN current_dataset d ON d.dataset = i.dataset \
       CROSS JOIN generate_series(0, 99) AS c(i) \
       WHERE c.i < json_length(i.cast_failures, 'checked') \
       GROUP BY i.table_name, i.imported_at), \
     cols AS ( \
       SELECT c.table_name, count(*) AS columns \
       FROM information_schema.columns c \
       JOIN current_dataset d ON d.dataset = c.table_schema \
       GROUP BY c.table_name) \
     SELECT l.table_name AS name, \
            coalesce(c.columns, 0) AS columns, \
            coalesce(CAST(i.landed_rows AS BIGINT), 0) AS rows, \
            coalesce(n.nulled, 0) AS nulled, \
            l.imported_at AS landed \
     FROM latest l \
     JOIN imports i ON i.table_name = l.table_name AND i.imported_at = l.imported_at \
     JOIN current_dataset d ON d.dataset = i.dataset \
     LEFT JOIN nulled n ON n.table_name = l.table_name AND n.imported_at = l.imported_at \
     LEFT JOIN cols c ON c.table_name = l.table_name \
     ORDER BY rows DESC, name";

/// Every metric of the bound dataset with where it stands — the
/// shipped read, whole.
pub(crate) const METRICS: &str =
    "SELECT name, title, kind, unit, meaning, grounded, stopped FROM metric_surfaces ORDER BY name";

/// The reader: human standing like every frame, one id for both pages.
fn reader() -> Actor {
    Actor {
        kind: ActorKind::Human,
        id: "app:workspace".into(),
    }
}

/// One read as JSON rows — every column by name, numbers as numbers.
/// A read that fails answers with its text; the page prints it in the
/// band it was for, the way a frame error lands in its tile.
pub(crate) async fn rows(
    door: &AppDoor,
    dataset: Option<&str>,
    sql: &str,
    params: &[(&str, &str)],
) -> Result<Vec<Value>, String> {
    let session = door
        .plane
        .channel(reader(), dataset)
        .await
        .map_err(|e| e.to_string())?;
    let values: HashMap<String, ScalarValue> = params
        .iter()
        .map(|(k, v)| (k.to_string(), ScalarValue::Utf8(Some(v.to_string()))))
        .collect();
    let params = (!values.is_empty()).then(|| ParamValues::from(values));
    let query = session
        .query_stream_with_params(sql, params)
        .await
        .map_err(|e| e.to_string())?;
    let mut stream = query.stream;
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
    }
    writer.finish().map_err(|e| e.to_string())?;
    let bytes = writer.into_inner();
    // No batch at all leaves the writer silent rather than at `[]`.
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

/// Digits grouped for reading (`412,338`), beside each named integer
/// field as `<field>_txt` — tera has no grouping filter, and the
/// template should place numbers, not format them.
pub(crate) fn grouped(rows: &mut [Value], fields: &[&str]) {
    for row in rows.iter_mut() {
        let Some(map) = row.as_object_mut() else {
            continue;
        };
        for field in fields {
            let text = map
                .get(*field)
                .and_then(Value::as_i64)
                .map(group_digits)
                .unwrap_or_default();
            map.insert(format!("{field}_txt"), Value::String(text));
        }
    }
}

fn group_digits(n: i64) -> String {
    let digits = n.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 {
        out.insert(0, '-');
    }
    out
}

/// `2026-09-04T14:10:03.512Z` read as `2026-09-04 14:10`: the minute
/// is what a landing time is for on a page.
pub(crate) fn minute(rows: &mut [Value], field: &str) {
    for row in rows.iter_mut() {
        let Some(map) = row.as_object_mut() else {
            continue;
        };
        let text = map
            .get(field)
            .and_then(Value::as_str)
            .map(|s| s.chars().take(16).collect::<String>().replace('T', " "))
            .unwrap_or_default();
        map.insert(format!("{field}_txt"), Value::String(text));
    }
}
