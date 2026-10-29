# The chat plan

How a result reaches a person inside the chat, and how a ruling gets
back. Two roads exist. Which one a person and an agent use well is
not measured, so this plan is an experiment with one measure, not a
build list. The trial runs on what stands today: the agent writes a
link to the docket and the person follows it. The shared frame is
`kernel.md`; the trial's road is `clients.md`.

## The two roads

**A page beside the chat.** The result names a page when something
waits on the person: a question to rule on, files to add. The agent
opens it. In Cowork the page opens in a pane beside the chat and
stays signed in on that computer; in claude.ai, Claude Desktop's
Chat, Claude for Excel and ChatGPT the same link opens the person's
browser. The page records the ruling with the cookie's standing. A
stream keeps the page current while the agent works.

**A card under the tool call.** The MCP Apps standard: a tool's
listing names a `ui://` HTML resource in `_meta.ui.resourceUri`; the
host renders it in a sandboxed frame under the call and hands it the
result's `structuredContent`. Claude on web, desktop, mobile and
Cowork and ChatGPT render it; Claude Code does not. A card appears
only when the model calls the tool; the server cannot push one, and
the standard says nothing about replacing an earlier card. A click in
a card reaches the server as a tool call with agent standing, so a
ruling from a card is still a link to the page. A card holds four or
five values and two actions; a result over about 150,000 characters
never reaches it.

## What stands

- Every result closes with `situation:` and `next:`
  (`crates/serverd/src/window.rs`, assembled in `mcp.rs:1246`).
  `next:` carries `next://` links for the model and nothing a person
  can open. The door does not hold the server's address: `own_uri`
  reaches the app routers (`lib.rs:184`) and not the door.
- The forms the door asks with render in Claude Code and not in
  claude.ai; the door skips them when the client lacks the capability
  (`mcp.rs:1085`, `question_round`), and the questions reach the
  person as prose. The model cannot record an answer: the agent door
  stamps agent standing, and SPEC.md §1 keeps an agent from speaking
  for a human.
- The docket records a ruling with the cookie's standing
  (`crates/apps/src/rule.rs`). A page refreshes only after its own
  write: `HX-Trigger: glossql:written` (`rule.rs:59`),
  `glossql:remeasured` beside it (`remeasure.rs:31`); the frame store
  evicts and the tiles refetch (`crates/apps/assets/store.js:124`).
  The store's version moves on any write
  (`crates/glossary/src/store.rs:1639`); nothing notifies on it. The
  page's security policy admits a same-origin stream (`pages.rs:32`).
- The results carry no `structuredContent`; the library in the tree
  has the fields (rmcp 3.5.1).
- glossval's agent lane keeps every door call of every scored
  session, so counting pages or cards per question is a query over
  what it has.
- In Cowork the agent can fill the ruling form with the person's
  cookie, and the server cannot tell that click from the person's.
  Accepted for the trial; the skill says hand over.

## The experiment

One measure for both roads, on scored sessions, on Sonnet and a GPT
model: does the agent put the thing in front of the person when
something waits and not otherwise, and does the ruling land. Counted:
calls per session, pages or cards opened per open question,
over-calls, rulings recorded.

| Road | The cheapest build that runs it | Effort |
|---|---|---|
| The page | an `open:` line on the result, built from `own_uri`, one page per result, absent when nothing waits; a paragraph in the skill: open it where the client has a browser beside the chat, hand out the link elsewhere, never fill the form | half a day |
| The card | `structuredContent` on the result; `_meta.ui.resourceUri` on the `glossql` tool naming one `ui://` page that shows the open questions and links the docket | one to two days |

The winning road gets its build after the measure. For the page: a
stream on the human door (`GET /<dataset>/app/events`, server-sent
events, `written` and `remeasured`, the page's listener; one to two
days) and the goals tile (the `next` read as a frame on the docket;
half a day to a day). For the card: the hosts' design rules, what a
repeated call does to earlier cards, a drop zone with a signed upload
link.

## Decisions that are the lead's

- Whether to run the card arm at all, or the page arm alone.
- The long-run answer to the agent filling the form in Cowork.

## Sources

- MCP Apps: https://modelcontextprotocol.io/extensions/apps/overview
- Claude's design rules and the character cap:
  https://claude.com/docs/connectors/building/mcp-apps/design-guidelines,
  https://claude.com/docs/connectors/building/mcp-apps/troubleshooting
- ChatGPT's UI layer: https://developers.openai.com/plugins/build/chatgpt-ui
- The browser beside the chat:
  https://support.claude.com/en/articles/16607400-use-the-built-in-browser-in-claude-cowork
- Forms per Claude surface:
  https://help.relativity.com/RelativityOne/Content/Relativity/MCP/Anthropic_Claude.htm
