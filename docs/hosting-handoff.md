# Handoff: hosting finalyze on the owner's VPS (a starting point for discussion)

You're picking up a conversation, not a spec. The owner and the
overhaul's lead agent talked this through and reached a direction they
like. Below is what was said and why, so you can carry the discussion on,
push back where something doesn't hold up, and handle the unexpected
sensibly. Nothing here is sealed. Treat it as the current best thinking,
and treat the owner as your partner in refining it.

## The app, briefly

`finalyze` is the owner's household finance app (several people's
accounts, treated as one household). It has a Python backend (FastAPI,
polars, one SQLite store) and a Svelte web app served by the same
process (`finalyze serve`). Code: the **`dev` branch** (the post-overhaul main line; `master`
is kept as the legacy version). Run it with plain `uv run finalyze` (the
CLI was removed, so this is the only command: it serves the app, with
`-C <config dir>`, `--host`, `--port` and `--demo`).
Start with its README, and `/agent.md`, which the running server serves:
the instance's API, written for agents.

How it's used today:
- The owner looks at the dashboard and annotates in the browser.
- A bookkeeping agent (a Claude Code session with its own
  workspace), reads and annotates through the HTTP API.
- The server binds to 127.0.0.1 only and has **no authentication**. Its
  only defense is that bind, plus a Host-header check against DNS
  rebinding. That was fine for a laptop. It isn't fine anywhere else.

The data is small and easy to name, and the app now separates three
kinds of state:
- **Data:**
  - raw exports (about 90 MB): the true source, one folder per account
    under the raw folder (`raw/<account>/`)
  - one SQLite store: everything authored
  - derived ingested CSVs
- **Preferences:** `config.toml` (ingestion rules, filters and so on). It
  holds **no paths**.
- **Host-specific:** `host.toml`, next to the config. Today it holds
  `data_dir`, `raw_dir`, `host` and `port`. It's never exported, and
  it's where the owner wants deployment secrets to live (see below).

Also relevant:
- **Backups:** named, in `<data_dir>/backups/`, listed in Settings with
  restore, download and delete; max 10, never pruned automatically.
- **Export and import:** a zip download and upload of data plus
  preferences, never the host file.
- **Raw files** are added by **uploading** them on the Ingest page (or
  `POST /api/raw/{account}`), which answers "how do exports get to the
  VPS".
- **No API endpoint takes a filesystem path:** the server reads and
  writes only inside its roots.
- **Cross-site writes from browsers are refused** (an Origin/Sec-Fetch
  check, because the server is unauthenticated). Check how this
  interacts with the proxy and the auth design.
- **The bookkeeping agent's workspace** (separate) has scripts that generate some asset
  CSVs.

## What the owner wants, and why

The owner runs many of their apps on a VPS (the domain's DNS is not on
Cloudflare). They reach it by
SSH and through an app for talking to agents, but otherwise it's
headless. They want two things:
1. **To see the dashboard from their phone and laptop, from anywhere:**
   on the road on a hotspot, on a friend's Wi-Fi, on cellular. Only their
   own devices, but from any network.
2. **To run agents, the bookkeeping agent especially, on the VPS,** next to the app,
   instead of only on the laptop.

They know this is extremely sensitive data, and that's exactly why it's
being thought through carefully, not just thrown up. They have accepted
some risks knowingly:
- the VPS provider can in principle read the data
- other apps on the same VPS share the machine; mitigate by isolating
  finalyze (its own user or container)
- an agent with shell access on the VPS can read the database directly,
  not just through the API

Don't re-litigate those unless something changes the picture. Do point
out anything new.

## What was considered

**A private network (Tailscale or WireGuard).** This was the lead's first
suggestion: the app reachable only over a mesh VPN between the owner's
devices and the VPS, so no public page exists to attack. The owner was
new to the idea. The lead clarified that it works from any network, and
that it wouldn't affect the owner's other public apps (only finalyze
would be private). The owner still **preferred plain public HTTPS**, as
long as it doesn't sacrifice security. That's a legitimate choice.
Revisit it only if the HTTPS design turns out to have a real hole.

**Rolling our own login in the app.** The owner's less sensitive apps
have hand-rolled logins, built by agents. For this app the lead argued
against it: authentication is security-critical code that a dedicated,
widely used tool does better. The owner's one regret is that a separate
login page doesn't look like the app. Authelia allows a logo, colors and
a title, not a full match. That trade was accepted for now.

**OAuth (log in with GitHub or Google).** The owner prefers not to.

## The direction they settled on

**Caddy (already the owner's reverse proxy, doing TLS for all their apps)
plus Authelia (self-hosted) via Caddy's `forward_auth`,** in front of this
one domain.
- Authelia gives a real login with 2FA: TOTP and passkeys (WebAuthn),
  which the owner liked. Passkeys on the phone in particular.
- It's scoped to finalyze's domain, but **the owner expects to reuse the
  same playbook for other sensitive apps**, so set it up in a way that
  generalizes: one Authelia, many protected domains, one login. That
  reuse is part of the value, so weigh choices with it in mind.

**The app accepts exactly two kinds of request, and refuses everything
else:**
1. **Browser traffic through Caddy, after Authelia.** Caddy adds a secret
   header only it knows, and the app refuses requests without it. That
   way, if the bare port were ever exposed by mistake, it's useless. The
   app also needs to trust the proxy's forwarded host and HTTPS for its
   Host check and for the URLs it writes into `/agent.md`.
2. **Agents, with a per-agent API token** (`Authorization: Bearer ...`).
   - The bookkeeping agent on the VPS keeps calling `http://127.0.0.1:...` directly, with
     its token. **Bare localhost without a token is refused too**, because
     on a shared VPS other apps and users can reach localhost.
   - An agent on the owner's laptop can use the public domain with its
     own token. For that, Caddy lets requests that carry a token skip the
     Authelia login and pass straight to the app, which checks the token.
   - That makes a token a full-access key reachable from the internet. So:
     **named, per-agent, revocable** tokens, shown once at creation,
     managed in the app's Settings, and stored hashed.
   - The owner asked how the bookkeeping agent would get its token: as a secret, e.g. a
     `FINALYZE_TOKEN` environment variable from a file only its user can
     read, with its primer saying to send it. Not pasted into a prompt.

**Identity.** Authelia tells Caddy who logged in (`Remote-User`,
`Remote-Groups`), and Caddy forwards it; the app trusts those headers only
alongside the proxy secret. Tokens identify agents the same way. The lead
suggested keeping this even though nothing needs permissions yet, because
it's nearly free and **pays off immediately for authorship.** An
annotation's author could come from who's logged in (e.g. a person's
username) or which token was used (e.g. the bookkeeping agent's), instead of an author typed into
each write. That lines up with a wishlist item from the owner and their agent
(authors as a list of contributors, and partial annotation updates).
See the overhaul case for the wishlist if you need it.

**Deliberately not now:**
- **Read-only access.** The owner asked whether several users with one of
  them read-only would be possible. Yes: a group in Authelia, and the app
  refusing writes for it. But they don't need it and are wary of the
  complexity. Agents don't need a read-only scope either. Leave room for
  it in the design; don't build it.

## Decided since this handoff was first written
- **Host-specific secrets live in the host layer:** the proxy secret and
  API tokens are never in the config, the store or exports. Today
  `host.toml` exists for paths; tokens may need their own host-local
  store, since they're created and revoked at runtime. Your call how.
- **Exports drop tokens.**
- **No token scopes for now:** agents can do everything a logged-in user
  can do, and vice versa. Exports and backups saved on the VPS are
  working as intended.

## Open questions worth talking through

- How finalyze runs on the VPS: its own user, systemd vs container, the
  host.toml paths, and how the owner deploys updates (the web app needs
  a build step, Node).
- Moving in: export on the laptop, then import (upload) on the VPS. The
  raw files then arrive by upload from the browser.
- The bookkeeping agent's move to the VPS: its workspace, primer, token, and the
  asset-CSV scripts it runs.
- Session length and passkey/TOTP enrollment in Authelia: what's
  comfortable on a phone without being lax.
- Rate limiting or lockout on the login and token paths. Authelia has
  regulation built in; token brute force is unrealistic with long random
  tokens, but worth a sentence.
- Whether the app should refuse to start without auth configured when
  bound to anything but localhost, so it fails safe.
- Anything the owner's other agent-built logins do that's worth
  matching or avoiding.

## Working style the owner expects

- **Explain why, discuss, then build.** The owner prefers understanding
  to instructions and will engage on the tradeoffs.
- Simplicity over machinery. The owner recently removed the whole CLI,
  a migration command and an anonymization mode because their upkeep
  outweighed their value. Expect the same skepticism toward anything elaborate.
- The repo is public: no personal data in code, tests or committed docs.
- The owner works in git worktrees and wants clean, committed work. Their
  global instructions cover the details.
