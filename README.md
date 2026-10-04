# anyscale-watchdog

Remote 24/7 watchdog for the Anyscale free-trial fleet (4 accounts × $100 credits).

Runs every **5 minutes** on GitHub Actions (public repo, free minutes). Each pass:

1. Polls `/api/v2/clouds` + AIOA waitlist status for all accounts.
2. On waitlist **CLAIMED** with no clouds: calls `claim_cloud` (bounded to 1/24h/account) and
   enables `system_cluster_config` on newly created clouds.
3. On any cloud reaching **ACTIVE**: deploys an OpenAI-compatible LLM service
   (`anyscale service deploy`, per-account `ANYSCALE_CLI_TOKEN`) — Qwen3-30B-A3B on H100 or
   Llama-3.1-8B on A10G depending on available instance types.

## Credentials — NEVER in the repo

The 4 Anyscale API keys live **only** in GitHub Secrets (encrypted):

| Secret     | Account email                      |
|------------|------------------------------------|
| `MARIE_KEY`| marie.v.argas0.02@gmail.com        |
| `JOSH_KEY` | josh.uamerab.ona.t@gmail.com       |
| `KER_KEY`  | ke.rasmia84@gmail.com              |
| `CLAZ_KEY` | cl.azedevou.re@gmail.com           |

State (`state.json`) + log (`watchdog.log`) are committed back to the repo after every pass
(claim-attempt bounds and deployed-cloud dedupe persist across runs).

## Files

- `watchdog-gha.mjs` — single-pass batch watchdog (Node 22, no deps).
- `.github/workflows/watchdog.yml` — cron `*/5` + manual dispatch; installs the anyscale CLI
  only for the deploy step.

## Manual trigger

`gh workflow run watchdog.yml` (or Actions tab → Run workflow).