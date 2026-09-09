"""Create a blob container on Azurite with the emulator's well-known
SharedKey — no SDK, stdlib only. `python3 azurite-container.py lake`;
the emulator's address in AZURITE_HOST (default 127.0.0.1:10000).
Waits for the emulator to listen, then answers 201 (created) or 409
(already there)."""

import base64, datetime, hashlib, hmac, os, sys, time, urllib.error, urllib.request

ACCOUNT = "devstoreaccount1"
KEY = base64.b64decode(
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
HOST = os.environ.get("AZURITE_HOST", "127.0.0.1:10000")


def create(container: str) -> int:
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    version = "2023-11-03"
    canonical_headers = f"x-ms-date:{now}\nx-ms-version:{version}\n"
    canonical_resource = f"/{ACCOUNT}/{ACCOUNT}/{container}\nrestype:container"
    string_to_sign = "PUT\n\n\n\n\n\n\n\n\n\n\n\n" + canonical_headers + canonical_resource
    signature = base64.b64encode(hmac.new(KEY, string_to_sign.encode(), hashlib.sha256).digest()).decode()
    req = urllib.request.Request(
        f"http://{HOST}/{ACCOUNT}/{container}?restype=container",
        method="PUT",
        headers={"x-ms-date": now, "x-ms-version": version, "Authorization": f"SharedKey {ACCOUNT}:{signature}", "Content-Length": "0"},
    )
    for attempt in range(60):
        try:
            with urllib.request.urlopen(req) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code
        except urllib.error.URLError:
            time.sleep(1)
    raise SystemExit(f"azurite at {HOST} did not answer")


if __name__ == "__main__":
    print(sys.argv[1], create(sys.argv[1]))
