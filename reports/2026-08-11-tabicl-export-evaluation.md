# TabICL → Rust serving: export evaluation

Date: 2026-08-11. Hands-on evaluation against tabicl 2.1.1 (site-packages at
`/Users/philipp/Code/dataraum/tfmeval/.venv/lib/python3.13/site-packages/tabicl`,
abbreviated `$T` below), torch 2.13.0, candle @ 6f74e7c (2026-08-06),
burn @ 89bcc85 / burn-onnx @ b59f483 (2026-08-07). Probe scripts under
`2026-08-11-tabicl-export-probes/` beside this file; they run in the
tfmeval venv (`uv run --project ../tfmeval python <probe>.py`).

## 1. Architecture

One raw torch module, `TabICL` (`$T/_model/tabicl.py:15`), three sequential
stages, shared by classifier and regressor (they differ only in config and
y-encoders):

1. **Column embedding** — `ColEmbedding` (`$T/_model/embedding.py:18`).
   Features are grouped by circular permutation into groups of 3
   (`feature_grouping`, embedding.py:206–242; shifts are `2**i % H`), 4 CLS
   slots are reserved by padding with the sentinel −100.0
   (embedding.py:467), each group row goes through `SkippableLinear` 3→128
   (`$T/_model/layers.py:92`; passes the −100 sentinel through), the train
   part gets a target embedding added (embedding.py:384–387; `nn.Linear(1,128)`
   for regression, `OneHotAndLinear(10,128)` for classification,
   embedding.py:168–172), then a **SetTransformer** of 3
   `InducedSelfAttentionBlock`s (ISAB, 128 inducing points;
   `$T/_model/encoders.py:208`, `$T/_model/layers.py:526`). Each ISAB = two
   pre-norm `MultiheadAttentionBlock`s: inducing points attend to the train
   rows only (layers.py:642–648), then all rows attend to the induced hidden.
   The first attention of each ISAB uses **QASSMax** query scaling
   (`$T/_model/ssmax.py:120`): `q * base_mlp(log n) * (1 + tanh(query_mlp(q)))`,
   elementwise per head dim. `affine=False` in the shipped configs, so the set
   transformer output is the embedding directly (embedding.py:412–417).
2. **Row interaction** — `RowInteraction` (`$T/_model/interaction.py:16`).
   Learnable 4×128 CLS tokens written into the reserved slots
   (interaction.py:193–194), a 3-block pre-norm transformer over the ~H+4
   tokens of each row with **RoPE** (non-interleaved half-split, theta=1e5;
   `$T/_model/rope.py:43–53`, applied to q and k in
   `$T/_model/attention.py:237–239`); the last block cross-attends: q = the 4
   CLS tokens, k/v = full sequence (interaction.py:149–162). Output = LN +
   concat of the 4 CLS outputs → 512-d row representation.
3. **In-context learning** — `ICLearning` (`$T/_model/learning.py:16`).
   y-encoder adds the train targets to the train rows' representations
   (learning.py:263–268), then a 12-block pre-norm transformer at d=512
   (ff=1024, GELU) where **all rows attend only to the first `train_size`
   rows** (layers.py:430–435: `k = v = q[..., :train_size, :]`), with QASSMax.
   Final LN + decoder MLP 512→1024→`out_dim` (learning.py:113):
   out_dim = 10 classes (classifier) or 999 quantiles (regressor). Test rows
   are sliced off after `train_size` (learning.py:323–328).

Attention itself is `F.scaled_dot_product_attention` over flattened batch
dims (attention.py:87–120); flash-attn-3 is an optional CUDA-only fast path
(attention.py:12–17, 99).

### Torch forward vs. Python orchestration

The eval-mode forward (`tabicl.py:495–506`) routes every stage through an
`InferenceManager` (`$T/_model/inference.py:517`): memory estimation, batch
splitting, CPU/disk offloading, AMP and FA3 toggles, pinned-buffer pools.
Pure serving infrastructure, untraceable by design. The train-mode forward
(`tabicl.py:291–345`) is the same math without the manager (dropout is 0.0
in both shipped configs), which is what the export probes used.

**Python-side orchestration that no ONNX export of the forward will capture**
(this is the porting surface):

- **Input encoding** — `TransformToNumerical` (`$T/_sklearn/preprocessing.py:57`):
  OrdinalEncoder for categorical/text/bool columns, mean imputation for
  numeric (DataFrame path); classifier label encoding via sklearn
  `LabelEncoder` (`$T/_sklearn/classifier.py:470–473`).
- **Target scaling (regressor)** — sklearn `StandardScaler` on y at fit
  (`$T/_sklearn/regressor.py:412–413`); inverse transform of every output
  at predict (regressor.py:762–770). All model outputs live in standardized-y
  space.
- **Ensembling** — `EnsembleGenerator` (`$T/_sklearn/preprocessing.py:877`):
  per norm-method (`['none','power']` by default, preprocessing.py:905) a
  `PreprocessingPipeline` (preprocessing.py:609) = `UniqueFeatureFilter` →
  `OutlierRemover` (z-threshold 4.0, preprocessing.py:237) → normalization
  (**sklearn `PowerTransformer` yeo-johnson** with standardize,
  preprocessing.py:679–680, or Quantile/RTDLQuantile variants) →
  `CustomStandardScaler` clipped to ±100 (preprocessing.py:359); plus
  feature-order shuffles (`Shuffler`, preprocessing.py:743, latin squares /
  random) and, for classification, class-label shuffles. Predictions are
  averaged across all members after inverting the class shuffles
  (classifier.py:771–797, incl. numpy softmax base.py:418–446;
  regressor.py:755–776: inverse-scale then mean over estimators).
  The `feature_shuffles` fast path (embed once, reorder embeddings per
  member) is a Python loop (embedding.py:638–673).
- **Regression distribution head** — `forward` returns 999 raw quantiles;
  `predict_stats` (tabicl.py:510–599) builds a `QuantileDistribution`
  (`$T/_model/quantile_dist.py:507`): monotonicity enforcement (default
  method "sort", quantile_dist.py:1478–1543; the isotonic-PAVA alternative is
  numba-JIT, quantile_dist.py:140+, and numba is not even installed in this
  venv — the fallback is `torch.sort`, quantile_dist.py:115–121),
  exponential- or GPD-tail parameter estimation (quantile_dist.py:284, 372),
  spline icdf/cdf/log_prob. mean/variance are direct moments of the sorted
  quantiles; median and what-if bands go through `dist.icdf`
  (tabicl.py:579–594).
- **Many-class classification** — > 10 classes: mixed-radix digit ensembling
  in the embedder (Python loop over digits, embedding.py:389–410) and a
  recursive hierarchical classification tree with per-node model calls,
  `torch.unique`/`searchsorted` label re-encoding, and probability chain-rule
  combination (learning.py:166–237, 334–407). Not exportable at all.
- **KV / representation caching** — `forward_with_cache` plumbing across all
  stages (tabicl.py:601–783, `$T/_model/kv_cache.py`), cache build loops in
  the wrappers (classifier.py:524–556, regressor.py:442–474). Performance
  machinery, not needed for correctness.
- **Unsupervised** — `TabICLUnsupervised` (`$T/_unsupervised/unsupervised.py:14`)
  is orchestration only: joint density by probability chain rule, one
  conditional TabICL classifier or regressor per column per random
  permutation, averaged over permutations (score_samples,
  unsupervised.py:183–228; `_compute_log_density` at 504). Numeric columns
  use `QuantileDistribution.log_prob`; categorical use class probabilities.
  Imputation and generation are further loops on the same machinery.

## 2. Checkpoints

Location: `~/.cache/huggingface/hub/models--jingang--TabICL/snapshots/4dcd344ece2c00be9e831fdd35bed57b5ad83e19/`
(HF repo `jingang/TabICL`; defaults `tabicl-classifier-v2-20260212.ckpt`,
classifier.py:290, and `tabicl-regressor-v2-20260212.ckpt`, regressor.py:249).

- Format: `torch.save` zip archives (loadable with `weights_only=True`, so no
  arbitrary pickle code), two top-level keys: `config` (kwargs for
  `TabICL(...)`) and `state_dict`.
- Classifier: 110,368,038 bytes on disk; 391 tensors; **27,552,258 params**,
  all float32. Config: max_classes=10, embed_dim=128, col 3 ISAB blocks /
  128 inducing points, row 3 blocks / 4 CLS, icl 12 blocks, qassmax-mlp-
  elementwise everywhere, feature_group "same" size 3.
- Regressor: 114,324,594 bytes; 347 tensors; **28,544,991 params**, all
  float32. Same config except max_classes=0, num_quantiles=999,
  bias_free_ln=true. Split: col_embedder 0.87M, row_interactor 0.40M,
  icl_predictor 27.27M.
- Safetensors conversion: **mechanical and verified** — all tensors plain
  float32, none sparse/quantized, zero shared storage;
  `safetensors.torch.save` succeeded in-memory (110.3 MB / 114.2 MB).
  120 of the regressor's 347 tensors belong to ssmax MLPs (the nonstandard
  part a hand-port must get right).

## 3. ONNX export probes

Environment constraint: the venv has **no `onnx`, `onnxscript`, or
`onnxruntime`** and installing was out of scope. This blocks serialization,
not graph conversion, and the probes were designed around that.

Probe target: regressor checkpoint, train-mode forward (see §1 for why),
X (1,10,4), y_train (1,7); sanity eager forward OK, output (1,3,999).

**Path A — TorchScript exporter (`torch.onnx.export(dynamo=False)`), static
and dynamic-axes variants:** both ran through tracing **and full ONNX graph
conversion** (no unsupported-op errors) and failed only at the final
protobuf post-processing step:

```
File ".../torch/onnx/_internal/torchscript_exporter/utils.py", line 1583, in _export
    proto = onnx_proto_utils._add_onnxscript_fn(
File ".../torchscript_exporter/onnx_proto_utils.py", line 185, in _add_onnxscript_fn
    raise errors.OnnxExporterError("Module onnx is not installed!") from e
torch.onnx.OnnxExporterError: Module onnx is not installed!
```

i.e. with the `onnx` package present this export would produce a file. The
tracer emitted the standard warnings that flag baked constants:

```
TracerWarning: Converting a tensor to a Python boolean might cause the trace to be incorrect. ...
TracerWarning: Converting a tensor to a Python number might cause the trace to be incorrect. ...
TracerWarning: Converting a tensor to a Python float might cause the trace to be incorrect. ...
TracerWarning: torch.tensor results are registered as constants in the trace. ...
```

at `embedding.py:378` (`int(y_train.max().item())`), `layers.py:134`
(`if skip_mask.any()`), and three hits of `ssmax.py:11`
(`torch.tensor(math.log(max(n, 1)))`).

**Path B — dynamo exporter (`dynamo=True`):** cannot start here:

```
File ".../torch/onnx/_internal/exporter/_core.py", line 19, in <module>
    import onnxscript
ModuleNotFoundError: No module named 'onnxscript'
```

To separate "missing package" from "fundamental", the same capture level was
probed with `torch.export.export` (needs no onnx package). **strict and
dynamic-shapes variants both fail on genuine data-dependent control flow**,
first at:

```
File ".../tabicl/_model/layers.py", line 134, in forward
    if skip_mask.any():
```

(SkippableLinear's sentinel branch; the ISAB skip branch at layers.py:670–677
is the same pattern one op later). So the dynamo path is blocked by model
code, not tooling — it would need upstream patches (`torch.cond` or removing
the sentinel branches) before onnxscript availability even matters.

**Trace-fidelity experiment (the decisive one).** `torch.jit.trace` at
(T=10, H=4, train=7), then traced-vs-eager at other shapes:

```
same shape (T=10,H=4,train=7)      : max abs diff = 0  (shapes (1, 3, 999))
more rows (T=16,H=4,train=7)       : max abs diff = 0  (shapes (1, 9, 999))
more train (T=10,H=4,train=8)      : max abs diff = 0.202977  (shapes (1, 2, 999))
more features (T=10,H=5,train=7)   : max abs diff = 0  (shapes (1, 3, 999))
```

The trace generalizes over row count and feature count, but **changing
`train_size` produces silently wrong numbers** (0.2 abs on standardized-y
quantiles is a large error). Cause: `train_size` enters as a Python int —
`log(train_size)` is frozen into the QASSMax scale tensors (ssmax.py:11
via attention `k.size(-2)`), and the train/test split slices bake. A
dynamic-axes ONNX file inherits exactly this: **one exported graph is only
correct for the train_size it was traced at.** For a serving system where
the in-context set varies per request that means re-exporting per context
size (requires Python at runtime) or context-size bucketing — and bucketing
by row padding is unsafe for this model: the −100 skip convention is
per-column (layers.py:133, 670), not per-row, and ISAB inducing points
attend to *all* train rows, so pad rows would contaminate the context.
This is a fundamental limitation of the trace-based path, independent of
which Rust ONNX consumer sits on the other end.

## 4. candle-onnx

`candle/candle-onnx/src/eval.rs` implements ~80 ops (interpreter over
candle tensors): Add/Mul/…, MatMul, Gemm, Softmax, Gather, GatherElements,
ScatterND, Where, If, OneHot, Range, Slice, Pad, Expand, Erf, Gelu,
ReduceMean/Max/Min/Sum, Trilu, LSTM/RNN, etc. **Missing for a TabICL trace:
`Einsum` (RoPE's freq application, rope.py uses einops/einsum), `NonZero`
(what boolean-mask `index_put` like `out[skip_mask] = -100` typically lowers
to), fused `LayerNormalization` (opset-17 form; opset ≤16 decomposition
would work), and `Loop`.** The crate's own README is a bare "adds ONNX
support to candle"; it is a side crate maintained at proof-of-concept
breadth, mostly exercised on vision models (candle-examples onnx = squeezenet
/ mobilenet). Even if the export existed, landing it on candle-onnx would
mean patching ops upstream — and §3's train_size problem remains. Not a
viable primary path.

## 5. candle hand-port

Everything the forward needs exists as first-class candle machinery:

- Weights: `VarBuilder::from_mmaped_safetensors` (used by
  `candle-examples/examples/bert/main.rs:90`); §2 shows the state dicts
  convert to safetensors mechanically.
- Blocks: `candle_nn::{linear, layer_norm, ops::softmax}`;
  `candle_nn::rotary_emb::rope` (`candle-nn/src/rotary_emb.rs:555`) is
  exactly the non-interleaved half-split variant the checkpoints use
  (`rope_i` at :262 is the interleaved one); `candle_nn::ops::sdpa`
  (`candle-nn/src/ops.rs:1308`) with Metal kernels plus a CPU flash-attention
  path (`candle-nn/src/cpu_flash_attention.rs`), or plain
  matmul+softmax+matmul as in the candle llama/bert examples;
  `candle_nn::encoding::one_hot` for the classifier y-encoder.
- Metal: a real backend (`Device::new_metal`, `candle-core/src/device.rs:258`,
  `candle-metal-kernels` crate); CPU backend uses Accelerate on macs. f32
  transformer inference on Metal is within candle's well-trodden zone; the
  caveat is that odd shapes/ops occasionally fall back or miss kernels, so
  the fidelity gate should run on both CPU and Metal.

What a port must implement (≈1.5–2k lines of model code, all eager, so
`train_size` is just a runtime argument — the trace-poisoning problem
disappears by construction):

1. `MultiheadAttention` with the packed `in_proj_weight` layout
   (attention.py:233: q,k,v split of one 3E×E matrix), optional RoPE on q/k,
   optional QASSMax query scaling.
2. `QASSMaxMLP` (ssmax.py:120): two 2-layer GELU MLPs, elementwise variant;
   pure tensor math, log(n) computed at runtime.
3. Pre-norm `MultiheadAttentionBlock` incl. the `train_size` k/v-slicing
   mode (layers.py:430–435) and the CLS-query cross-attention call used by
   the last row block.
4. ISAB (two blocks + inducing-point parameter) and the 3-block
   SetTransformer; the −100 skip logic can be implemented literally (mask +
   where) or engineered away, since in a controlled server the sentinel
   pattern is fully determined by the fixed 4-CLS layout.
5. `ColEmbedding`: circular feature grouping (gather with precomputed
   indices), pad, y-embedding add.
6. `RowInteraction`: CLS write-in, RoPE encoder, concat.
7. `ICLearning`: y-encoder add, 12-block encoder, LN, decoder, test-slice.

Plus the orchestration from §1 for parity with the sklearn wrapper:
y StandardScaler (trivial), ensemble generation — the one genuinely annoying
piece is sklearn's yeo-johnson `PowerTransformer` (per-column lambda via MLE
/ Brent optimization); options are porting it (self-contained 1-D
optimization) or starting with `norm_methods=['none']` + z-clip and measuring
the accuracy cost — and the quantile head: sort-based monotonicity +
exp-tail `QuantileDistribution.icdf` for bands/median (the numba PAVA path is
optional; this venv runs the sort fallback anyway). The InferenceManager is
*replaced*, not ported (Rust server owns batching/memory).

## 6. burn

burn's ONNX import has moved out of the main repo into `tracel-ai/burn-onnx`
(the main repo's `burn-book/src/onnx-import.md` documents it); it generates
Rust source from the ONNX graph at build time rather than interpreting it.
Coverage is far ahead of candle-onnx: `SUPPORTED-ONNX-OPS.md` lists 169
supported ops including Einsum, LayerNormalization, NonZero, ScatterND,
OneHot, If, Loop, Where, TopK, and a fused Attention — essentially
everything a decomposed TabICL trace could contain (torch's
`Unique` → ONNX Unique is unsupported, but that only appears in the
hierarchical many-class path, which isn't exportable anyway). Recommended
opset ≥16. If an ONNX route were ever forced, burn-onnx is the credible
consumer, not candle-onnx. It does not, however, cure the §3 problem: the
ONNX file it consumes is still train_size-specialized, and build-time code
generation makes per-context re-export even less practical. A burn hand-port
is possible too (burn-nn has the same primitives) but offers no advantage
over candle here, and the team's stack alignment (candle is the lighter,
inference-first library) favors candle.

## 7. Recommendation

**Hand-port the TabICL forward to candle; do not pursue ONNX.** Grounds:

- The only mechanically-clean export (TS trace) produces graphs that are
  silently wrong when `train_size` differs from trace time (measured 0.203
  max abs error) — and varying in-context sets is precisely the product's
  usage pattern. The dynamo/torch.export path fails today on data-dependent
  branches in tabicl's own code (`layers.py:134`).
- candle-onnx lacks ops the trace needs (Einsum, NonZero); burn-onnx covers
  them but inherits the same specialization defect.
- The model is small (28.5M params, three stages, ~7 layer types) and built
  entirely from blocks candle-nn already provides; the two nonstandard
  pieces (QASSMax, ISAB) are a few dozen lines of tensor math each. Weights
  convert to safetensors mechanically (verified in-memory for both
  checkpoints).

**Scope** (in order): safetensors conversion of both checkpoints → the seven
modules of §5 → regressor read-out (y-scaling, sort-monotonic quantile head,
mean/median/band extraction) → ensembling (start `norm_methods=['none']`,
n_estimators with feature shuffles; add yeo-johnson when the accuracy delta
demands it) → classifier read-out (≤10 classes: same blocks + OneHot
y-encoder + temperature softmax + class-shuffle averaging; defer the
hierarchical >10-class tree) → unsupervised density as a Rust orchestration
loop over the two cores plus `QuantileDistribution.log_prob`.

**Export-fidelity gate**: fixture-driven golden tests generated from Python.
Tier 1 — raw forward: fixed (X, y_train) at several (T, H, train_size)
combinations through the train-mode Python forward vs. the Rust forward;
compare the raw 999-quantile / 10-logit tensors, tolerance ~1e-4 fp32
(CPU and Metal). Tier 2 — wrapper parity with ensembling pinned
(n_estimators=1, norm='none', shuffles disabled): compare
`predict(output_type=["mean","median","quantiles"])` end to end, which
additionally locks the y-scaler and the quantile-distribution head. Tier 3 —
full-default ensemble on the finance-generator data, compared statistically
(rank correlation / band coverage), since ensemble RNG streams won't be
bit-identical across languages.

**Read-outs by path**: the candle hand-port can serve all three — regressor
(what-if bands: quantile head over the ported forward), classifier (≤10
classes), and unsupervised density/ranking (orchestration over both). An
ONNX path, had it worked, would cover only the two raw forwards at fixed
train_size; ensembling, y-scaling, the quantile-distribution head, many-class
classification, and the entire unsupervised chain-rule read live outside the
exportable graph in every scenario. The two the product needs most —
regressor and density — are respectively "forward + a well-defined numeric
head" and "pure orchestration", both fully reachable from the hand-port and
both out of reach of any export-only route.
