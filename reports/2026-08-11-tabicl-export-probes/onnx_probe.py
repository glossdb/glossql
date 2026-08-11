"""Probe ONNX exportability of the core TabICL torch module (regressor config).

The clean tensor path is the train-mode forward (dropout=0.0 everywhere, so
train-mode math == eval-mode math for regression); eval mode routes through
InferenceManager (Python-side batching/offloading) which is untraceable by design.
"""
import sys, traceback, warnings
import torch
from tabicl._model.tabicl import TabICL

BASE = "/Users/philipp/.cache/huggingface/hub/models--jingang--TabICL/snapshots/4dcd344ece2c00be9e831fdd35bed57b5ad83e19"
CKPT = f"{BASE}/tabicl-regressor-v2-20260212.ckpt"
OUT = "/private/tmp/claude-501/-Users-philipp-Code-dataraum-glossql/caeefcd4-f601-4784-b410-762936d10c18/scratchpad/tabicl_reg.onnx"

ckpt = torch.load(CKPT, map_location="cpu", weights_only=True)
model = TabICL(**ckpt["config"])
model.load_state_dict(ckpt["state_dict"])
model.train()  # selects _train_forward: pure tensor path, no InferenceManager

B, T, H, TRAIN = 1, 10, 4, 7
X = torch.randn(B, T, H)
y_train = torch.randn(B, TRAIN)

# sanity: does the forward run at all on CPU?
with torch.no_grad():
    out = model(X, y_train)
print("sanity forward OK, output shape:", tuple(out.shape))


def attempt(name, fn):
    print("\n" + "=" * 70)
    print("ATTEMPT:", name)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("always")
            fn()
        print("RESULT: SUCCESS")
    except Exception as e:
        print(f"RESULT: FAILED with {type(e).__module__}.{type(e).__name__}")
        tb = traceback.format_exc()
        lines = tb.splitlines()
        if len(lines) > 40:
            tb = "\n".join(lines[:15] + ["  ... <traceback trimmed> ..."] + lines[-20:])
        print(tb)


# --- Path A: legacy TorchScript exporter ---
attempt("torch.onnx.export dynamo=False, static shapes", lambda: torch.onnx.export(
    model, (X, y_train), OUT, dynamo=False, opset_version=18,
    input_names=["X", "y_train"], output_names=["quantiles"],
))

attempt("torch.onnx.export dynamo=False, dynamic axes", lambda: torch.onnx.export(
    model, (X, y_train), OUT, dynamo=False, opset_version=18,
    input_names=["X", "y_train"], output_names=["quantiles"],
    dynamic_axes={"X": {1: "rows", 2: "features"}, "y_train": {1: "train_rows"}},
))

# --- Path B: dynamo exporter ---
attempt("torch.onnx.export dynamo=True", lambda: torch.onnx.export(
    model, (X, y_train), OUT, dynamo=True, opset_version=18,
))
