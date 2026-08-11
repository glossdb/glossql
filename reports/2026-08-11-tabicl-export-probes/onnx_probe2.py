"""Probe 2: capture tracer warnings from the TS export path (they enumerate the
constants baked into the trace), and test dynamo-level graph capture via
torch.export.export (needs no onnx package)."""
import traceback, warnings
import torch
from tabicl._model.tabicl import TabICL

BASE = "/Users/philipp/.cache/huggingface/hub/models--jingang--TabICL/snapshots/4dcd344ece2c00be9e831fdd35bed57b5ad83e19"
ckpt = torch.load(f"{BASE}/tabicl-regressor-v2-20260212.ckpt", map_location="cpu", weights_only=True)
model = TabICL(**ckpt["config"])
model.load_state_dict(ckpt["state_dict"])
model.train()

B, T, H, TRAIN = 1, 10, 4, 7
X = torch.randn(B, T, H)
y_train = torch.randn(B, TRAIN)

print("### TS trace warnings (dedup) ###")
with warnings.catch_warnings(record=True) as ws:
    warnings.simplefilter("always")
    try:
        torch.onnx.export(model, (X, y_train), "/dev/null", dynamo=False, opset_version=18)
    except Exception as e:
        print("(export tail failure as before:", type(e).__name__, str(e)[:80], ")")
seen = set()
for w in ws:
    key = (str(w.category.__name__), str(w.message)[:200])
    if key in seen:
        continue
    seen.add(key)
    print(f"- {w.category.__name__}: {str(w.message)[:300]}")

print("\n### torch.export.export (dynamo graph capture, strict) ###")
try:
    ep = torch.export.export(model, (X, y_train), strict=True)
    print("SUCCESS:", len(ep.graph.nodes), "graph nodes")
except Exception:
    tb = traceback.format_exc().splitlines()
    print("\n".join(tb[:6] + ["  ..."] + tb[-12:]))

print("\n### torch.export.export with dynamic shapes ###")
from torch.export import Dim
try:
    rows = Dim("rows", min=4, max=4096)
    feats = Dim("feats", min=1, max=256)
    trows = Dim("trows", min=2, max=4096)
    ep = torch.export.export(
        model, (X, y_train), strict=True,
        dynamic_shapes={"X": {1: rows, 2: feats}, "y_train": {1: trows}},
    )
    print("SUCCESS:", len(ep.graph.nodes), "graph nodes")
except Exception:
    tb = traceback.format_exc().splitlines()
    print("\n".join(tb[:6] + ["  ..."] + tb[-14:]))
