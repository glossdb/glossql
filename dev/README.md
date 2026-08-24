# Development credential

A public key and two tokens for running the server locally.

```bash
serverd --workspace ~/acme --public-key dev/public.pem --issuer glossql-dev
```

- `agent.jwt` — an MCP client's `Authorization: Bearer` header.
- `human.jwt` — a browser's `glossql_token` cookie.

Both name the audience `http://127.0.0.1:8080` and expire 2036-08-20.

**These are committed on purpose.** They carry no secret: the private
half that signed them was generated for this, used once, and deleted.
Nothing in this repository can sign, which is the property worth
keeping — a resource server that can mint tokens is an authorization
server, whatever it is called. `auth.rs` has a test asserting no
private key ever appears here.

Deployments do not use these. Point `--public-key` at your IdP's key
and `--issuer` at its issuer.

## Minting a replacement

Any Ed25519 keypair and any JWT tool will do; with openssl alone:

```bash
openssl genpkey -algorithm ed25519 -out private.pem
openssl pkey -in private.pem -pubout -out dev/public.pem
b64() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
hdr=$(printf '{"alg":"EdDSA","typ":"JWT"}' | b64)
iat=$(date -u +%s); exp=$(( iat + 3650*86400 ))
for kind in human agent; do
  pay=$(printf '{"iss":"glossql-dev","aud":"http://127.0.0.1:8080","sub":"dev-%s","kind":"%s","exp":%s,"iat":%s}' \
        "$kind" "$kind" "$exp" "$iat" | b64)
  printf '%s.%s' "$hdr" "$pay" > signing
  sig=$(openssl pkeyutl -sign -inkey private.pem -rawin -in signing | b64)
  printf '%s.%s.%s' "$hdr" "$pay" "$sig" > "dev/$kind.jwt"
done
rm -f private.pem signing
```

The last line is the one that matters.
