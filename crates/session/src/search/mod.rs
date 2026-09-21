//! The search doors. A search enumerates candidates over an arbitrary
//! table's columns, which a static SQL body cannot spell: the input
//! schema varies while the output schema is fixed. So a search
//! computes in the pre-pass like every compute door — over the
//! statement's own pins, so it can never straddle a landing — and
//! optimizes recall: no thresholds here, the judgment lives in the
//! measurement body that reads the door. One module per door family;
//! what they share — the shape decoder, the detector's state, the
//! dataset's pins — is here.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::DataFusionError;
use datafusion::common::config::ConfigNonZeroUsize;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use serde_json::Value;

use crate::reads::Shared;
use crate::session::SessionError;

mod bands;
mod coherence;
mod collisions;
mod derivations;
mod facts;
mod hierarchies;
mod relationships;

pub(crate) use bands::{band_points, metric_band_walk, monthly_sql};
pub(crate) use coherence::relationship_checks;
pub(crate) use collisions::{
    QuerySlot, current_fact_values, current_query_slots, grounding_collisions,
};
pub(crate) use derivations::derivation_candidates;
pub(crate) use facts::{fact_values, metric_sources};
pub(crate) use hierarchies::hierarchy_candidates;
pub(crate) use relationships::relationship_candidates;

/// A named Int64 column of a one-batch result, materialized.
pub(crate) fn int_column(
    batches: &[RecordBatch],
    name: &str,
) -> datafusion::common::Result<Vec<i64>> {
    let b = batches.iter().find(|b| b.num_rows() > 0).ok_or_else(|| {
        DataFusionError::Execution(format!("`{name}`: the plan returned nothing"))
    })?;
    let idx = b.schema().index_of(name)?;
    let a = b
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal(format!("`{name}` did not read as an integer")))?;
    Ok((0..b.num_rows()).map(|r| a.value(r)).collect())
}

/// A door's fixed shape, decoded from JSON rows through the format's
/// own decoder — the same trick the profile aggregate uses, so there is
/// no hand-built array assembly to drift. No rows is the empty
/// relation, never a refusal.
pub(crate) fn rows_batch(
    rows: Vec<Value>,
    fields: Vec<Field>,
) -> Result<RecordBatch, SessionError> {
    let schema = Arc::new(Schema::new(fields));
    let mut decoder = arrow_json::ReaderBuilder::new(Arc::clone(&schema))
        .build_decoder()
        .map_err(SessionError::from)?;
    decoder.serialize(&rows).map_err(SessionError::from)?;
    // The decoder flushes nothing for no rows; a door with nothing to
    // say serves the empty relation in its own shape.
    Ok(decoder
        .flush()
        .map_err(SessionError::from)?
        .unwrap_or_else(|| RecordBatch::new_empty(schema)))
}

/// The named dataset's tables, each pinned at its current snapshot.
/// A dataset door's argument names what it reads: the dataset in use
/// is the statement's binding, not the door's, and a run before any
/// `USE` — `SELECT detect_relationships() FROM fin` — reads `fin`
/// rather than the empty binding.
async fn dataset_pins(
    shared: &Arc<Shared>,
    dataset: &str,
) -> Result<crate::prepass::Resolved, SessionError> {
    Ok(crate::prepass::Resolved::over(
        shared
            .pinned(dataset)
            .await?
            .iter()
            .map(|p| (p.name.clone(), Arc::clone(&p.provider)))
            .collect(),
    ))
}

/// The detector's own state: a merge join, not a hash join, over four
/// partitions, with a small batch.
///
/// A hash join reserves its whole build side and refuses when the pool
/// is short — it has no spill path at all, only a `try_grow` that
/// returns the error (datafusion-physical-plan
/// `joins/hash_join/exec.rs`, the build-side fold), and its consumer
/// registers as unspillable, so what it holds also shrinks the share
/// every spilling operator gets. A pair pass joins a column's distinct
/// values to themselves, so the build side is the data: a door that
/// refuses on a wide dataset is the failure this pass exists to
/// remove. `SortMergeJoinExec` spills. The price is a sort per side,
/// which the plan metrics below report.
///
/// The fair pool grants every spilling operator instance the same
/// share: the pool less the unspillable reservations, divided by the
/// instances registered (datafusion-execution `memory_pool/pool.rs`,
/// `FairSpillPool::try_grow`). A pass registers one instance per
/// partition per union arm and per sort, and both sides of the
/// self-join plan the union again, so the instance count scales with
/// the table count times the partition count. A final-mode aggregate
/// that has to spill first reserves headroom the size of its state
/// (datafusion-physical-plan `aggregates/row_hash.rs`,
/// `update_memory_reservation`), and its first state is one unnested
/// input batch — a batch of rows times the arm's column count — which
/// must fit the share or the spill itself is refused. The batch size
/// is what sizes that first reservation, so the state runs a batch of
/// 1024 rows: at 8192 a 115-column table's first reservation exceeded
/// the share under four partitions with most of the pool free, at
/// 1024 it fits with room for wider tables. The partition count stays
/// at four: fewer partitions widen the share but slow every pass in
/// proportion, and at one the planner plans a collect-left hash join
/// whatever the preference (datafusion `physical_planner.rs`, the
/// `target_partitions() > 1` arm).
fn detector_state(ctx: &SessionContext) -> SessionState {
    let state = ctx.state();
    let mut config = state.config().clone();
    config.options_mut().optimizer.prefer_hash_join = false;
    config.options_mut().execution.target_partitions = 4;
    // The first reservation of a final aggregate is one input batch of
    // state; on a wide unnest that is rows × columns.
    config.options_mut().execution.batch_size =
        ConfigNonZeroUsize::try_new(1024).expect("a literal above zero");
    SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build()
}

/// A plan through the given state: planned, optimized and collected
/// under that state's configuration and the process's one pool.
async fn run_plan(
    state: &SessionState,
    plan: LogicalPlan,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let physical = state.create_physical_plan(&plan).await?;
    let started = std::time::Instant::now();
    let batches =
        datafusion::physical_plan::collect(Arc::clone(&physical), state.task_ctx()).await?;
    // The plan with its operators' metrics — rows, compute, spills —
    // for a door's own passes, which the statement's `executed` line
    // does not see.
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        metrics = %datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(physical.as_ref())
            .indent(false),
        "door plan"
    );
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::NullEquality;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{JoinType, LogicalPlanBuilder};
    use datafusion::physical_plan::displayable;

    use super::*;

    /// The detector's state plans a join as a merge, not a hash: the
    /// pair pass joins a column's distinct values to themselves, and a
    /// hash join's build side has no spill path — it refuses instead.
    /// The knob is on the state, so this is what keeps it plumbed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_detector_joins_by_merge_so_the_pass_can_spill() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let provider: Arc<dyn datafusion::catalog::TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let side = |alias: &str| {
            LogicalPlanBuilder::scan("t", provider_as_source(Arc::clone(&provider)), None)
                .and_then(|b| b.alias(alias))
                .and_then(|b| b.build())
                .unwrap()
        };
        let plan = LogicalPlanBuilder::from(side("a"))
            .join_detailed(
                side("b"),
                JoinType::Inner,
                (vec!["a.v"], vec!["b.v"]),
                None,
                NullEquality::NullEqualsNothing,
            )
            .and_then(|b| b.build())
            .unwrap();

        let state = detector_state(&ctx);
        let physical = state.create_physical_plan(&plan).await.unwrap();
        let rendered = displayable(physical.as_ref()).indent(false).to_string();
        assert!(
            rendered.contains("SortMergeJoin"),
            "the detector's join is a merge join:\n{rendered}"
        );
        assert!(
            !rendered.contains("HashJoin"),
            "no hash join in the detector's plan:\n{rendered}"
        );
    }
}
