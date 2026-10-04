// watchdog-gha.mjs -- Anyscale fleet watchdog, GitHub Actions batch variant (2026-10-04)
// Runs ONE full pass over all accounts per invocation (cron */5 on GHA).
// - Polls clouds + AIOA waitlist status for each account.
// - If waitlist flips to CLAIMED and no clouds: claim_cloud (bounded 1/24h/account) + enables
//   system_cluster_config on new clouds.
// - The moment ANY cloud is ACTIVE: deploys an OpenAI-compatible LLM service via the anyscale CLI
//   using ANYSCALE_CLI_TOKEN = that account's key (per-account token).
// - Crash-safe: state.json + watchdog.log are committed back to the repo so runs are idempotent.
// - KEYS COME FROM ENV ONLY (GitHub Secrets MARIE_KEY/JOSH_KEY/KER_KEY/CLAZ_KEY). Never file-persisted.
import fs from "node:fs";
import { spawnSync } from "node:child_process";

const STATE = "state.json";
const LOG = "watchdog.log";
const API = "https://console.anyscale.com/api/v2";
const ACCOUNTS = [
  { name: "marie", key: process.env.MARIE_KEY || "" },
  { name: "josh", key: process.env.JOSH_KEY || "" },
  { name: "ker", key: process.env.KER_KEY || "" },
  { name: "claz", key: process.env.CLAZ_KEY || "" },
].filter((a) => a.key);

const log = (m) => {
  const l = new Date().toISOString() + " " + m;
  console.log(l);
  try { fs.appendFileSync(LOG, l + "\n"); } catch {}
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function loadState() {
  try { return JSON.parse(fs.readFileSync(STATE, "utf8")); }
  catch { return { deployed: {}, claimAttempt: {}, lastPoll: null, lastStatus: {}, lastHeartbeat: 0 }; }
}
function saveState(s) {
  try { fs.writeFileSync(STATE, JSON.stringify(s, null, 2)); } catch {}
}
async function jget(url, key) {
  try { const r = await fetch(url, { headers: { Authorization: "Bearer " + key } }); const t = await r.text(); let j = null; try { j = JSON.parse(t); } catch {} return { status: r.status, j, text: t.slice(0, 200) }; }
  catch (e) { return { status: 0, j: null, text: "net:" + e.message }; }
}
async function jpost(url, key, body) {
  try { const r = await fetch(url, { method: "POST", headers: { Authorization: "Bearer " + key, "Content-Type": "application/json" }, body: body ? JSON.stringify(body) : undefined }); const t = await r.text(); let j = null; try { j = JSON.parse(t); } catch {} return { status: r.status, j, text: t.slice(0, 200) }; }
  catch (e) { return { status: 0, j: null, text: "net:" + e.message }; }
}
async function jput(url, key) {
  try { const r = await fetch(url, { method: "PUT", headers: { Authorization: "Bearer " + key } }); const t = await r.text(); let j = null; try { j = JSON.parse(t); } catch {} return { status: r.status, j, text: t.slice(0, 200) }; }
  catch (e) { return { status: 0, j: null, text: "net:" + e.message }; }
}
function pickModel(types) {
  const t = (Array.isArray(types) && types.length) ? types[0] : null;
  const gpu = t ? (t.gpu_type || t.name || "") : "";
  if (/h100|h200|a100|h200/i.test(gpu)) return { model: "hotpizzatactics/Qwen3-30B-A3B-abliterated-FP8-dynamic", accelerator: "H100", maxLen: 16384, tp: 1 };
  return { model: "meta-llama/Llama-3.1-8B-Instruct", accelerator: "A10G", maxLen: 8192, tp: 1 };
}
function deploy(acct, cloud) {
  const cfg = pickModel(cloud.types);
  log("DEPLOYING on " + acct.name + "/" + cloud.name + " (" + cloud.id + ") with " + JSON.stringify(cfg));
  const servePy = 'import os\nfrom ray.serve.llm import LLMConfig, build_openai_app\nmodel_source = os.environ.get("MODEL_SOURCE", "' + cfg.model + '")\naccelerator = os.environ.get("ACCELERATOR_TYPE", "' + cfg.accelerator + '")\nmax_len = int(os.environ.get("MAX_MODEL_LEN", "' + cfg.maxLen + '"))\ntp = int(os.environ.get("TENSOR_PARALLEL", "' + cfg.tp + '"))\ngpu_util = float(os.environ.get("GPU_MEM_UTIL", "0.90"))\nllm_config = LLMConfig(\n    model_loading_config=dict(model_id=os.path.basename(model_source), model_source=model_source),\n    accelerator_type=accelerator,\n    deployment_config=dict(autoscaling_config=dict(min_replicas=1, max_replicas=1)),\n    engine_kwargs=dict(tensor_parallel_size=tp, max_model_len=max_len, gpu_memory_utilization=gpu_util, max_num_seqs=256, enable_prefix_caching=True),\n)\napp = build_openai_app({"llm_configs": [llm_config]})\n';
  const yaml = "name: gonka-" + Date.now() + "\nimage_uri: anyscale/ray-llm:2.58.0-py312-cu130\ncompute_config:\n  auto_select_worker_config: true\nworking_dir: .\ncloud: " + cloud.id + "\napplications:\n  - import_path: serve_adaptive:app\n";
  fs.mkdirSync("deploy", { recursive: true });
  fs.writeFileSync("deploy/serve_adaptive.py", servePy);
  fs.writeFileSync("deploy/service.yaml", yaml);
  const env = Object.assign({}, process.env, { ANYSCALE_CLI_TOKEN: acct.key, NO_COLOR: "1" });
  const res = spawnSync("anyscale", ["service", "deploy", "-f", "service.yaml"], { cwd: "deploy", env, encoding: "utf8", timeout: 10 * 60 * 1000 });
  const out = (res.stdout || "") + (res.stderr || "");
  log("deploy exit=" + (res.status ?? "sig" + res.signal) + ": " + out.slice(0, 1200));
  return res.status === 0 || /Starting new service/i.test(out);
}
function gitCommitPush() {
  const env = Object.assign({}, process.env, { GIT_TERMINAL_PROMPT: "0" });
  const run = (args) => { const r = spawnSync("git", args, { env, encoding: "utf8" }); if (r.status !== 0) log("git " + args[0] + " -> " + (r.stdout || "") + (r.stderr || "")); return r.status === 0; };
  run(["config", "user.email", "watchdog-bot@users.noreply.github.com"]);
  run(["config", "user.name", "anyscale-watchdog-bot"]);
  run(["add", STATE, LOG]);
  const c = spawnSync("git", ["commit", "-m", "watchdog state " + new Date().toISOString()], { env, encoding: "utf8" });
  if (c.status === 0 || /nothing to commit/i.test(c.stdout + c.stderr)) {
    const repo = process.env.GITHUB_REPOSITORY;
    const token = process.env.GITHUB_TOKEN;
    if (repo && token) {
      run(["push", "https://x-access-token:" + token + "@github.com/" + repo + ".git", "HEAD:" + (process.env.GITHUB_REF_NAME || "main")]);
    }
  }
}

(async () => {
  log("watchdog-gha batch pass (accounts=" + ACCOUNTS.length + ")");
  if (!ACCOUNTS.length) { log("NO KEYS — check GitHub Secrets MARIE_KEY/JOSH_KEY/KER_KEY/CLAZ_KEY"); gitCommitPush(); process.exit(1); }
  const state = loadState();
  for (const acct of ACCOUNTS) {
    const cl = await jget(API + "/clouds", acct.key);
    let clouds = [];
    if (cl.status === 200 && cl.j) clouds = cl.j.results || [];
    const ws = await jget(API + "/aioa_cloud_waitlist/check_status", acct.key);
    const st = ws.j && ws.j.result ? (ws.j.result.status || JSON.stringify(ws.j.result)) : (ws.status === 200 ? "?" : "err" + ws.status);
    if (state.lastStatus[acct.name] !== st) { log("waitlist " + acct.name + ": " + state.lastStatus[acct.name] + " -> " + st); state.lastStatus[acct.name] = st; }
    if (st === "CLAIMED" && clouds.length === 0) {
      const last = state.claimAttempt[acct.name] ? new Date(state.claimAttempt[acct.name]).getTime() : 0;
      if (Date.now() - last > 24 * 3600 * 1000) {
        state.claimAttempt[acct.name] = new Date().toISOString();
        const cc = await jpost(API + "/aioa_cloud_waitlist/claim_cloud", acct.key, null);
        log("claim_cloud " + acct.name + " -> " + cc.status + " " + cc.text.slice(0, 120));
        await sleep(2000);
        const cl2 = await jget(API + "/clouds", acct.key);
        clouds = (cl2.status === 200 && cl2.j && cl2.j.results) || clouds;
        for (const c of clouds) {
          if (c.state === "CREATING" && !c.system_cluster_config_id) {
            const up = await jput(API + "/clouds/" + c.id + "/update_system_cluster_config?is_enabled=true", acct.key);
            log("syscfg enable " + acct.name + "/" + c.id + " -> " + up.status + " " + up.text.slice(0, 80));
          }
        }
      }
    }
    for (const c of clouds) {
      if (c.state === "ACTIVE" && !state.deployed[c.id]) {
        let types = [];
        try { const r2 = await jget(API + "/clouds/" + c.id + "/additional_instance_types", acct.key); types = (r2.j && r2.j.results) || []; } catch {}
        log("ACTIVE FOUND: " + acct.name + "/" + c.name + " types=" + JSON.stringify(types.slice(0, 3)));
        const ok = deploy(acct, { id: c.id, name: c.name, types });
        if (ok) { state.deployed[c.id] = new Date().toISOString(); log("SUCCESS deployed on " + c.id); }
      }
    }
    const counts = {};
    for (const c of clouds) counts[c.state] = (counts[c.state] || 0) + 1;
    log("status " + acct.name + ": waitlist=" + st + " clouds=" + clouds.length + " " + JSON.stringify(counts));
  }
  state.lastPoll = new Date().toISOString();
  saveState(state);
  // trim log to last 800 lines
  try { const lines = fs.readFileSync(LOG, "utf8").split("\n"); if (lines.length > 800) fs.writeFileSync(LOG, lines.slice(-800).join("\n")); } catch {}
  gitCommitPush();
  log("pass done");
  process.exit(0);
})().catch((e) => { log("FATAL " + e.stack); gitCommitPush(); process.exit(1); });