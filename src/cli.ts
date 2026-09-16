#!/usr/bin/env node
// mineru-cloud: the same tools as the MCP server, driven from a shell.
// It runs the MCP server in-process over an in-memory transport and calls its
// tools, so the CLI and the server can never drift apart.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import createServer from "./index.js";

const PREFIX = "mineru_";
const POLL_MS = 10_000;
const WAIT_MAX_MS = 30 * 60_000;

function usage(tools: Array<{ name: string; description?: string }>): string {
  const lines = [
    "mineru-cloud — MinerU cloud API from the shell (same tools as the mineru MCP server)",
    "",
    "usage: mineru-cloud <command> [--option value ...] [--wait]",
    "       mineru-cloud list",
    "",
    "commands:",
  ];
  for (const t of tools) {
    const cmd = t.name.replace(PREFIX, "").replace(/_/g, "-");
    lines.push(`  ${cmd.padEnd(18)} ${(t.description || "").split(/\.\s/)[0]}`);
  }
  lines.push(
    "",
    "options mirror the tool's parameters (--url, --pages, --total-pages, --output-dir, ...).",
    "values: numbers and true/false are coerced; JSON arrays/objects are parsed.",
    "--wait re-runs a status/merge/download command every 10s until nothing is still processing.",
    "env: MINERU_API_KEY (required), MINERU_BASE_URL, MINERU_DEFAULT_MODEL",
  );
  return lines.join("\n");
}

function coerce(v: string): unknown {
  if (v === "true") return true;
  if (v === "false") return false;
  if (/^-?\d+(\.\d+)?$/.test(v)) return Number(v);
  if (/^[\[{]/.test(v)) {
    try { return JSON.parse(v); } catch { /* keep string */ }
  }
  return v;
}

function parseArgs(argv: string[]): { command: string | undefined; args: Record<string, unknown>; wait: boolean } {
  const [command, ...rest] = argv;
  const args: Record<string, unknown> = {};
  let wait = false;
  for (let i = 0; i < rest.length; i++) {
    const a = rest[i];
    if (a === "--wait") { wait = true; continue; }
    if (!a.startsWith("--")) throw new Error(`Unexpected argument: ${a}`);
    let key = a.slice(2);
    let val: string | undefined;
    const eq = key.indexOf("=");
    if (eq >= 0) { val = key.slice(eq + 1); key = key.slice(0, eq); }
    else if (i + 1 < rest.length && !rest[i + 1].startsWith("--")) { val = rest[++i]; }
    else { val = "true"; }
    args[key.replace(/-/g, "_")] = coerce(val);
  }
  return { command, args, wait };
}

const STILL_WORKING = /still processing|still running|pending|\brunning\b|converting|Try again later|Re-run/i;
const FINISHED = /^(done|failed)\b|^Merged|Downloaded to:/m;

async function main() {
  const { command, args, wait } = parseArgs(process.argv.slice(2));

  const server = createServer({
    config: {
      mineruApiKey: process.env.MINERU_API_KEY || "",
      mineruBaseUrl: process.env.MINERU_BASE_URL || "https://mineru.net/api/v4",
      mineruDefaultModel: (process.env.MINERU_DEFAULT_MODEL as "pipeline" | "vlm") || "pipeline",
    },
  });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new Client({ name: "mineru-cloud", version: "1.0.0" });
  await client.connect(clientTransport);
  const { tools } = await client.listTools();

  if (!command || command === "help" || command === "--help" || command === "-h") {
    console.log(usage(tools));
    return;
  }
  if (command === "list") {
    for (const t of tools) {
      const cmd = t.name.replace(PREFIX, "").replace(/_/g, "-");
      const props = Object.entries((t.inputSchema as { properties?: Record<string, { description?: string }> }).properties || {});
      console.log(`${cmd}\n  ${t.description}\n` + props.map(([k, p]) => `  --${k.replace(/_/g, "-")}  ${p.description || ""}`).join("\n"));
    }
    return;
  }

  const toolName = PREFIX + command.replace(/-/g, "_");
  if (!tools.some((t) => t.name === toolName)) {
    throw new Error(`Unknown command '${command}'. Run 'mineru-cloud list'.`);
  }

  const started = Date.now();
  for (;;) {
    const result = await client.callTool({ name: toolName, arguments: args });
    const text = (result.content as Array<{ type: string; text?: string }>)
      .filter((c) => c.type === "text").map((c) => c.text || "").join("\n");
    if (result.isError) throw new Error(text);
    const stillWorking = wait && STILL_WORKING.test(text) && !FINISHED.test(text);
    if (!stillWorking) { console.log(text); return; }
    if (Date.now() - started > WAIT_MAX_MS) throw new Error(`Gave up waiting after 30 min:\n${text}`);
    process.stderr.write(`[wait] ${text.split("\n")[0].slice(0, 100)}\n`);
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
