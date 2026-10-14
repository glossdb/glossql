//! A grounding planned through the session's own pipeline, and the
//! time-column test every monthly reader shares.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;

use crate::reads::Shared;
use crate::session::SessionError;

/// Plan a query through the session's own pipeline, so a nested `read.`
/// re-enters the relation planner. Every door that plans a grounding —
/// the cube, the walk, the misfit frame, the searches — plans through
/// this one gate.
pub(crate) fn build_plan<'a>(
    shared: &'a Arc<Shared>,
    ctx: &'a SessionContext,
    sql: &'a str,
) -> futures::future::BoxFuture<'a, Result<LogicalPlan, SessionError>> {
    // Boxed here, once: the pre-pass under it makes this a large
    // future, and every door that plans a grounding awaits it.
    Box::pin(async move {
        // Parsed once, under the pre-pass's one-query rule; the same
        // statement is resolved and then planned.
        let query = crate::prepass::parse(sql, "the grounding")?;
        let mut statement = datafusion::sql::parser::Statement::Statement(Box::new(
            SQLStatement::Query(Box::new(query)),
        ));
        // A grounding body may name `read.<x>()`; resolve before planning.
        let resolved = crate::prepass::resolve(shared, ctx, &mut statement).await?;
        crate::reads::state_with(ctx, shared, resolved)
            .statement_to_plan(statement)
            .await
            .map_err(SessionError::not_served)
    })
}

/// The one temporal-column test every monthly reader shares.
pub(crate) fn is_temporal(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

/// The first date-typed column, the same discovery every reader does.
pub(crate) fn date_column(fields: &Fields) -> Option<String> {
    fields
        .iter()
        .find(|f| is_temporal(f.data_type()))
        .map(|f| f.name().clone())
}
