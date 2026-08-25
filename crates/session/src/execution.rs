//! A read's execution as the engine reports it. DataFusion counts per
//! operator — rows out, compute time, spills — behind
//! `ExecutionPlan::metrics()`, the seam `EXPLAIN ANALYZE` reads. This
//! module surfaces those counts into the read's span when its stream
//! ends, complete or dropped; nothing here counts anything itself.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor, visit_execution_plan};
use futures::Stream;

/// What the operators counted, summed over the plan.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Summary {
    pub operators: usize,
    /// Nanoseconds the operators spent computing, summed: CPU time on
    /// the runtime, never wall time — the span's busy time is that.
    pub elapsed_compute_ns: usize,
    pub spill_count: usize,
    pub spilled_bytes: usize,
}

/// The plan's metrics, summed over every operator that reports any.
/// Complete once every partition's stream has ended
/// (`ExecutionPlan::metrics`); read earlier, it is what has run so far.
pub(crate) fn summary(plan: &dyn ExecutionPlan) -> Summary {
    struct Sum(Summary);
    impl ExecutionPlanVisitor for Sum {
        type Error = std::convert::Infallible;
        fn pre_visit(
            &mut self,
            plan: &dyn ExecutionPlan,
        ) -> std::result::Result<bool, Self::Error> {
            self.0.operators += 1;
            if let Some(metrics) = plan.metrics() {
                let m = metrics.aggregate_by_name();
                self.0.elapsed_compute_ns += m.elapsed_compute().unwrap_or(0);
                self.0.spill_count += m.spill_count().unwrap_or(0);
                self.0.spilled_bytes += m.spilled_bytes().unwrap_or(0);
            }
            Ok(true)
        }
    }
    let mut sum = Sum(Summary::default());
    let Ok(()) = visit_execution_plan(plan, &mut sum);
    sum.0
}

/// The read's stream, holding the read's span so it closes when the
/// stream ends — the client took every row, or dropped it, which is the
/// cancellation contract: the work stops, and the record says how far
/// it got and what the operators counted.
pub(crate) struct Metered {
    inner: SendableRecordBatchStream,
    plan: Arc<dyn ExecutionPlan>,
    span: tracing::Span,
    rows: usize,
    complete: bool,
}

impl Metered {
    pub(crate) fn new(
        inner: SendableRecordBatchStream,
        plan: Arc<dyn ExecutionPlan>,
        span: tracing::Span,
    ) -> Self {
        Metered {
            inner,
            plan,
            span,
            rows: 0,
            complete: false,
        }
    }
}

impl Stream for Metered {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Entered for the poll and no longer — a poll has no await point
        // — so the span's busy time is the streaming's compute as well as
        // the planning's, and its idle time is the wait on the client
        // and the engine.
        let _entered = this.span.enter();
        let next = this.inner.as_mut().poll_next(cx);
        match &next {
            Poll::Ready(Some(Ok(batch))) => this.rows += batch.num_rows(),
            Poll::Ready(None) => this.complete = true,
            _ => {}
        }
        next
    }
}

impl RecordBatchStream for Metered {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for Metered {
    fn drop(&mut self) {
        let s = summary(self.plan.as_ref());
        tracing::info!(
            parent: &self.span,
            rows = self.rows,
            complete = self.complete,
            operators = s.operators,
            compute_ms = s.elapsed_compute_ns as f64 / 1e6,
            spill_count = s.spill_count,
            spilled_bytes = s.spilled_bytes,
            "executed"
        );
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                parent: &self.span,
                plan = %DisplayableExecutionPlan::with_metrics(self.plan.as_ref()).indent(false),
                "the plan, with what its operators counted"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    /// The summary reads what the operators counted: nothing before the
    /// stream ran, compute time and the row count after — and a stream
    /// drained to its end says so.
    #[tokio::test]
    async fn the_summary_reads_what_the_operators_counted() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(
                (0..10_000).collect::<Vec<i64>>(),
            ))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let plan = ctx
            .sql("SELECT v % 7 AS k, sum(v) FROM t GROUP BY k")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let before = super::summary(plan.as_ref());
        assert!(before.operators >= 2, "{before:?}");
        assert_eq!(before.elapsed_compute_ns, 0, "{before:?}");

        let stream = execute_stream(Arc::clone(&plan), ctx.task_ctx()).unwrap();
        let mut metered = super::Metered::new(stream, Arc::clone(&plan), tracing::Span::none());
        let mut rows = 0;
        while let Some(batch) = metered.next().await {
            rows += batch.unwrap().num_rows();
        }
        assert_eq!(rows, 7);
        assert_eq!((metered.rows, metered.complete), (7, true));
        let after = super::summary(plan.as_ref());
        assert!(after.elapsed_compute_ns > 0, "{after:?}");
        assert_eq!((after.spill_count, after.spilled_bytes), (0, 0));
    }
}
