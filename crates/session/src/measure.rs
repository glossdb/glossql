//! A measurement is a query.
//! Role by shape decides the body language: a function with
//! `RETURNS` carries SQL, a judge carries a script. The helpers here are
//! the SQL half — recognizing the body, binding the subject into its
//! AST, and shaping the executed result into the JSON that lands.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, Statement as SQLStatement, Value as SqlValue, visit_expressions_mut,
};
use glossql_parser::{GlossqlParser, Statement};
use serde_json::Value;

use crate::session::SessionError;

/// The body as one substrate query, if that is what it is. Anything
/// else — prose, a statement sequence — is `None`, and each caller
/// refuses with the shape its role owes.
pub(crate) fn sql_body(script: &str) -> Option<DFStatement> {
    let mut statements = GlossqlParser::parse_sql(script).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Statement::Substrate(statement) = statements.pop()? else {
        return None;
    };
    matches!(&*statement, DFStatement::Statement(inner)
        if matches!(inner.as_ref(), SQLStatement::Query(_)))
    .then(|| *statement)
}

/// Replace every `$subject` placeholder with the subject as a string
/// literal — an AST node, so the value cannot re-tokenize whatever it
/// carries. Expansion, not string splicing; the derive-generated visitor
/// reaches table-function arguments like any other expression.
pub(crate) fn bind_subject(statement: &mut DFStatement, subject: &str) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = visit_expressions_mut(inner.as_mut(), |expr| {
        if let Expr::Value(v) = expr
            && let SqlValue::Placeholder(p) = &v.value
            && p == "$subject"
        {
            v.value = SqlValue::SingleQuotedString(subject.to_string());
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
}

/// Bind named string parameters the same way, before the pre-pass runs —
/// so a door argument (`metric_series(grain => $grain)`) resolves like any quoted
/// literal. Same expansion-not-splicing guarantee as [`bind_subject`]:
/// the value lands as one AST node and cannot re-tokenize. Only string
/// values bind here; anything else stays a placeholder for the plan's
/// own parameter pass, and a placeholder nobody bound still fails at
/// execution.
pub(crate) fn bind_params(
    statement: &mut DFStatement,
    params: &std::collections::HashMap<String, datafusion::common::metadata::ScalarAndMetadata>,
) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = visit_expressions_mut(inner.as_mut(), |expr| {
        if let Expr::Value(v) = expr
            && let SqlValue::Placeholder(p) = &v.value
            && let Some(datafusion::common::ScalarValue::Utf8(Some(s))) = p
                .strip_prefix('$')
                .and_then(|name| params.get(name))
                .map(|p| &p.value)
        {
            v.value = SqlValue::SingleQuotedString(s.clone());
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
}

/// The result-shape rule: one row × one column → the
/// value itself; one row → an object of its columns; anything else → an
/// array of row objects. NULL keys are omitted — the writer's default,
/// and checked against the record: no golden body carries a null key.
pub(crate) fn body_value(batches: &[RecordBatch]) -> Result<Value, SessionError> {
    fn shape(e: impl std::fmt::Display) -> SessionError {
        SessionError::Runtime(format!("the result does not shape: {e}"))
    }
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    for batch in batches {
        writer.write(batch).map_err(shape)?;
    }
    writer.finish().map_err(shape)?;
    let text = writer.into_inner();
    let rows: Value = if text.is_empty() {
        Value::Array(Vec::new())
    } else {
        serde_json::from_slice(&text).map_err(shape)?
    };
    let Value::Array(mut rows) = rows else {
        return Err(shape("not rows"));
    };
    let width = batches.first().map_or(0, RecordBatch::num_columns);
    Ok(match (rows.len(), width) {
        (1, 1) => match rows.remove(0) {
            Value::Object(mut o) => o.values_mut().next().map(Value::take).unwrap_or(Value::Null),
            other => other,
        },
        (1, _) => rows.remove(0),
        _ => Value::Array(rows),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use serde_json::json;

    use super::*;

    fn batch(names: &[&str], columns: Vec<Arc<dyn datafusion::arrow::array::Array>>) -> RecordBatch {
        let fields: Vec<Field> = names
            .iter()
            .zip(&columns)
            .map(|(n, c)| Field::new(*n, c.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn the_result_shape_rule() {
        // one row × one column → the value itself
        let one = batch(&["n"], vec![Arc::new(Int64Array::from(vec![7]))]);
        assert_eq!(body_value(&[one]).unwrap(), json!(7));
        // one row → an object of its columns
        let row = batch(
            &["rate", "note"],
            vec![
                Arc::new(Float64Array::from(vec![0.5])),
                Arc::new(StringArray::from(vec!["fine"])),
            ],
        );
        assert_eq!(
            body_value(&[row]).unwrap(),
            json!({"rate": 0.5, "note": "fine"})
        );
        // many rows → an array of row objects
        let many = batch(&["n"], vec![Arc::new(Int64Array::from(vec![1, 2]))]);
        assert_eq!(body_value(&[many]).unwrap(), json!([{"n": 1}, {"n": 2}]));
        // nothing → an empty array; a lone NULL → null (its key is omitted)
        assert_eq!(body_value(&[]).unwrap(), json!([]));
        let null = batch(&["n"], vec![Arc::new(Int64Array::from(vec![None::<i64>]))]);
        assert_eq!(body_value(&[null]).unwrap(), Value::Null);
    }

    #[test]
    fn the_subject_binds_into_table_function_arguments() {
        let mut statement =
            sql_body("SELECT profile(v) FROM subject_column($subject) WHERE $subject <> ''")
                .expect("one query");
        bind_subject(&mut statement, "journal_lines.Unnamed: 0");
        let rendered = statement.to_string();
        assert!(
            rendered.contains("subject_column('journal_lines.Unnamed: 0')"),
            "{rendered}"
        );
        assert!(!rendered.contains("$subject"), "{rendered}");
    }

    #[test]
    fn role_by_shape_reads_the_body() {
        assert!(sql_body("SELECT 1").is_some());
        assert!(sql_body("-- a comment\nSELECT count(*) FROM t").is_some());
        assert!(sql_body("let x = 1; x + 1").is_none(), "a script is not SQL");
        assert!(sql_body("SELECT 1; SELECT 2").is_none(), "one query only");
        assert!(
            sql_body("INSERT INTO t VALUES (1)").is_none(),
            "a measurement reads"
        );
    }
}
