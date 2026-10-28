# The clients plan

What a person meets on the trial: sign up, connect Claude or
ChatGPT, put a file in, work. The shared frame is `kernel.md`. How a
result reaches the person inside the chat, and how a ruling gets
back, is `chat.md`. What a file is once it is in, a workbook or a
PDF, is `sources.md`; the road in comes before the readers, since a
reader helps nobody who cannot hand the file over. The platform is
`../glossdeploy/platform.md`; this plan names what glossql needs
from it and in what order. Build order is risk order: what can stop
the trial comes first.

## The table

| Element | Risk | What it touches | When | Issue |
|---|---|---|---|---|
| The spike: one workspace reachable at its own name from Claude Desktop and ChatGPT, end to end | High: nothing is built; every item is on platform.md's bring-up list | glossdeploy: the Worker route, the WorkOS wildcards; glossql: the next row | First; one to two days | |
| The organization check and the extra allowed host | Low | `crates/serverd/src/auth.rs`, `config.rs`, the install page | With the spike; one day | |
| Registration: a stranger gets a workspace without us | Medium: the order of the sign-up events is unverified; creation takes minutes | glossdeploy: the cockpit, cut down | After the spike; about five days | |
| The upload page on the human door | Low: the object store and the human writes exist | `crates/apps`, `crates/serverd` | After the spike; one to two days | |
| The docket's Glossary tab and Query tab | Low | `crates/apps` | Built with the demo; the trial's own pass after it runs end to end | #178, #179 |
| The shared connector address | Low once the spike stands: the same Worker, routing by the organization in the token instead of the hostname | glossdeploy; a second audience on the agent door | When the directory listing is near | |

## The spike

One workspace made by hand with the trial program, behind one
Cloudflare route on `<name>.glossdb.com`, the WorkOS wildcards set.
Then, through Claude Desktop and through ChatGPT: add the connector,
sign in, run a session, a streamed answer, a cold start from zero,
the browser sign-in on the docket at the same name. Recorded: the
handshake end to end; the cold-start time against each client's tool
timeout; whether the Worker streams the MCP session; whether
creation at sign-up or on the first call is the cheaper road. The
list is platform.md's "To verify at bring-up". What it decides: the
hostname road, the Worker or Cloud Run's domain mapping, and whether
the trial can open at all.

## What stands

- **One process, one address.** The address is the audience; a token
  must name it (`crates/serverd/src/auth.rs:226`). The server answers
  its own hostname and loopback (`config.rs:254`). Behind the
  Cloudflare route the Host header is the run.app host, so the door
  needs one more allowed host from the environment. `Claims` reads
  `sub` and `aud` (`auth.rs:137`), not the organization; with a
  wildcard audience over `*.glossdb.com`, any signed-up person could
  get a token for another workspace, so the gate must refuse a token
  whose `org_id` is not the workspace's, bearer and cookie alike.
  Both settings are named in platform.md and not in the code.
- **Connecting.** A connector is added by URL in Claude on every plan
  and in ChatGPT; both register themselves at the issuer. No deep
  link exists; the person pastes `https://<name>.glossdb.com/mcp`.
- **Registration.** glossql has no user table; it verifies the
  issuer's tokens. WorkOS AuthKit's hosted page does sign-up. A
  sign-up makes a person, not an organization; the application
  creates the organization after the first sign-in. A workspace is a
  service, a bucket and a database, two to four minutes to create.
  The events `user.created` and `organization.created` exist; their
  order on a fresh sign-up is not verified.
- **Files.** MCP carries no file bytes; in Claude an attached file
  goes to the model's sandbox, never to a tool. A file is a source
  once it sits under a location a `DECLARE SOURCE` names
  (`crates/import/src/lib.rs`, `list_source`); the GCP program
  exports `gs://<bucket>/sources`; the server opens the lake through
  the object store (`crates/catalog`); on a laptop it reads a
  directory (`config.rs`, `local_sources`). The app door takes human
  writes with the cookie's standing (`crates/apps/src/rule.rs`,
  `remeasure.rs`). So the upload page is a page of the docket with a
  drop zone and a PUT on the human door that streams into that
  location: no signing, every lake family, the laptop too.
- **The first session** is about 30 tool calls and runs well from
  Claude and ChatGPT. No preparation job and no onboarding command:
  the glossing needs the conversation.
- **Cold start.** The process starts in 0.24 s fresh on the laptop;
  Cloud Run's part is unmeasured. The spike measures it against the
  clients' tool timeouts.
- **Claude for Excel.** Connectors work the same in the Office
  add-ins; where its links open is not documented. A test when the
  trial stands.

## The cockpit, cut down

What the platform must do and nothing else: create the workspace,
tell the person how to connect, stop and delete it, show usage
against the caps, run the caps on a cron. So: sign-up on the identity
provider's hosted page; one status page per workspace, preparing or
ready, the connect lines for Claude, ChatGPT and Excel, the docket
link, usage, stop, delete; the caps on a cron. No upload page, since
it is the docket's; no import button, since the agent imports on the
first prompt; no organization page. About half of platform.md's
Worker.

## Decisions that are the lead's

- The cockpit's cut, above.
- Creation at sign-up by the webhook, or on the first call with a
  stand-in answer; the spike says which is cheaper.
- Whether boot declares the deployment's upload location as a
  standing source, so the structure goal's first step is done before
  the first prompt.
- A size cap on an upload, if any.

## Issue, drafted

### The organization check and the extra allowed host

- **What it buys.** A workspace behind a wildcard audience refuses
  tokens of other organizations, and the agent door answers behind a
  route whose Host is not the audience's.
- **What stands.** `Claims` reads `sub` and `aud`
  (`crates/serverd/src/auth.rs:137`); the audience check is one
  compare (`auth.rs:226`). The allowed hosts are loopback and the
  audience's host (`crates/serverd/src/config.rs:254`). The issuer's
  tokens carry `org_id`. `GLOSSQL_ORGANIZATION` and
  `GLOSSQL_ALLOWED_HOSTS` are named in the platform design and not in
  the code.
- **Done when.** With `GLOSSQL_ORGANIZATION` set, the gate refuses a
  token whose `org_id` differs and admits one that matches, bearer
  and cookie alike; unset, nothing changes. `GLOSSQL_ALLOWED_HOSTS`
  adds hosts to the allowed set. The install page names both. The
  suite holds a refused token, an admitted token, and a request on
  the extra host.

## For the other repos

- **glossdeploy**: the spike, then the cockpit cut down, the Worker
  route and the WorkOS wildcards. The organization check and the
  extra host land in glossql. The shared address, when it comes, is
  the same Worker routing by the token's organization and a stand-in
  answer while a workspace starts.
- **glossval**: nothing on this plan. The ranking experiment is
  `sources.md`'s; the recommendations read is `futures.md`'s.
