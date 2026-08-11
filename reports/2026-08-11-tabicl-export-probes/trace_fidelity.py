"""Does a TS trace of TabICL generalize beyond the traced (T, H, train_size)?
This is the fidelity question for any trace-based ONNX export."""
import traceback
import torch
from tabicl._model.tabicl import TabICL

BASE = "/Users/philipp/.cache/huggingface/hub/models--jingang--TabICL/snapshots/4dcd344ece2c00be9e831fdd35bed57b5ad83e19"
ckpt = torch.load(f"{BASE}/tabicl-regressor-v2-20260212.ckpt", map_location="cpu", weights_only=True)
model = TabICL(**ckpt["config"])
model.load_state_dict(ckpt["state_dict"])
model.train()

torch.manual_seed(0)
X0 = torch.randn(1, 10, 4)
y0 = torch.randn(1, 7)

with torch.no_grad():
    traced = torch.jit.trace(model, (X0, y0), check_trace=False)

def compare(tag, T, H, TR):
    X = torch.randn(1, T, H)
    y = torch.randn(1, TR)
    try:
        with torch.no_grad():
            eager = model(X, y)
            tr = traced(X, y)
        if eager.shape != tr.shape:
            print(f"{tag}: SHAPE MISMATCH eager {tuple(eager.shape)} vs traced {tuple(tr.shape)}")
        else:
            diff = (eager - tr).abs().max().item()
            print(f"{tag}: max abs diff = {diff:.6g}  (shapes {tuple(eager.shape)})")
    except Exception as e:
        msg = str(e).splitlines()[0][:220]
        print(f"{tag}: traced module RAISED {type(e).__name__}: {msg}")

compare("same shape (T=10,H=4,train=7)      ", 10, 4, 7)
compare("more rows (T=16,H=4,train=7)       ", 16, 4, 7)
compare("more train (T=10,H=4,train=8)      ", 10, 4, 8)
compare("more features (T=10,H=5,train=7)   ", 10, 5, 7)
