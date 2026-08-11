"""Inspect TabICL checkpoints: format, config, param count, safetensors convertibility."""
import torch, json, collections

BASE = "/Users/philipp/.cache/huggingface/hub/models--jingang--TabICL/snapshots/4dcd344ece2c00be9e831fdd35bed57b5ad83e19"
for name in ["tabicl-classifier-v2-20260212.ckpt", "tabicl-regressor-v2-20260212.ckpt"]:
    path = f"{BASE}/{name}"
    ckpt = torch.load(path, map_location="cpu", weights_only=True)
    print("=" * 70)
    print(name)
    print("top-level keys:", list(ckpt.keys()))
    print("config:", json.dumps(ckpt["config"], default=str))
    sd = ckpt["state_dict"]
    n_params = sum(v.numel() for v in sd.values())
    dtypes = collections.Counter(str(v.dtype) for v in sd.values())
    print(f"num state_dict tensors: {len(sd)}; total params: {n_params:,} ({n_params/1e6:.2f}M)")
    print("dtypes:", dict(dtypes))
    # plain tensors? (no sparse, no quantized, all contiguous-able)
    weird = [k for k, v in sd.items() if not isinstance(v, torch.Tensor) or v.is_sparse or v.is_quantized]
    print("non-plain tensors:", weird[:5] if weird else "none")
    # shared storage check (safetensors rejects shared storage)
    ptrs = collections.Counter(v.untyped_storage().data_ptr() for v in sd.values())
    shared = [p for p, c in ptrs.items() if c > 1]
    print("tensors sharing storage:", len(shared))
    # sample of key names
    keys = list(sd.keys())
    print("first keys:", keys[:8])
    print("last keys:", keys[-4:])
    # safetensors save attempt (in-memory)
    try:
        from safetensors.torch import save
        blob = save({k: v.contiguous() for k, v in sd.items()})
        print(f"safetensors serialize OK, {len(blob)/1e6:.1f} MB")
    except Exception as e:
        print("safetensors attempt failed:", type(e).__name__, e)
