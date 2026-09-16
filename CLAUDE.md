# mineru-mcp

MCP server for MinerU document parsing API — PDF/DOC/PPT/images to markdown.

## Quick Reference

- **Language**: TypeScript/Node
- **Package manager**: bun
- **Build**: `bun run build` (outputs to `dist/`)
- **Dev**: `bun run dev` (tsx, stdio mode)
- **Entry**: `src/index.ts` (stdio) / `src/server.ts` (HTTP) / `src/cli.ts` (`mineru-cloud` — runs the server in-process over `InMemoryTransport` and calls its tools; one implementation, two channels)

## API Key Management

- **Provider**: MinerU (OpenXLab) — https://mineru.net
- **Format**: Bearer token. Older keys were JWTs (decode `exp` below); keys issued in 2026 are opaque `sk-…` strings — the decode snippet then fails, and the only expiry check is a live probe: `GET /extract/task/probe` → `-60012` means authenticated, `401/403` means expired.
- **Expiry**: Tokens auto-expire after ~90 days from issuance
- **Don't hard-code the expiry date here** — a stale one is worse than none. (This line used to read "Current key expires: 2026-05-19" and sat ~2 months past that, presenting an expired key as current.) Read the real expiry from the token itself:
  ```bash
  # decode the JWT payload -> exp (unix seconds)
  python3 -c "import base64,json,os,sys;t=os.environ['MINERU_API_KEY'].split('.')[1];print(json.loads(base64.urlsafe_b64decode(t+'='*(-len(t)%4)))['exp'])"
  ```
- **Config location**: `~/.claude.json` under `mcpServers.mineru.env.MINERU_API_KEY` (appears in both global and project-level entries)
- **Env var**: `MINERU_API_KEY`
- **Troubleshooting 401**: almost always an expired token. Decode `exp` (above); tokens are **not refreshable** — generate a new one at mineru.net and update *both* the global and project-level entries in `~/.claude.json`.

## Architecture

Single-file server (`src/index.ts`, ~1000 lines) with 8 tools:

| Tool | Purpose | Flow |
|------|---------|------|
| `mineru_parse` | Parse single URL | Returns `task_id` |
| `mineru_status` | Check task progress | Poll with `task_id` |
| `mineru_batch` | Parse multiple URLs (preferred) | Returns `batch_id` |
| `mineru_batch_status` | Check batch progress | Poll with `batch_id` |
| `mineru_upload_batch` | Upload local files (slow, use URLs when possible) | Returns `batch_id` |
| `mineru_download_results` | Download named paper folders | Uses `batch_id`, saves to `output_dir` |
| `mineru_parse_long` | Document >200 pages | One batch of ≤200-page `page_ranges` slices; `data_id` = `name__pAAAAA-BBBBB` |
| `mineru_merge_slices` | Stitch a sliced batch | Orders by `data_id`, prefixes images per slice, re-bases `page_idx` |

### URL workflow (preferred)

```
mineru_batch (array of public URLs — arXiv, SSRN, publisher sites)
  → mineru_batch_status (poll until all done)
  → mineru_download_results (extracts named paper folders)
```

### Local file workflow (fallback)

```
mineru_upload_batch (directory or files — slow, may timeout)
  → mineru_batch_status (poll until all done)
  → mineru_download_results (extracts named paper folders)
```

### How upload works

1. Collects files from `directory` or `files` param
2. Requests presigned OSS upload URLs from `/file-urls/batch`
3. Uploads each file via PUT to presigned URL (native fetch, no Content-Type header)
4. Size-proportional timeout: 60s base + 2s per MB. On timeout, suggests switching to URL approach.
5. MinerU processes automatically; poll with `mineru_batch_status`

### How download works

1. Fetches batch results from API
2. Downloads each `.zip` result via streaming
3. Extracts with `unzip` CLI (requires `unzip` on PATH)
4. Creates named paper folder `{stem}/` in output directory
5. Copies `full.md` → `{stem}.md`, `content_list_v2.json` → `{stem}_content.json`, and `images/`
6. Skips all other files (layout.json, model.json, block_list.json, origin PDF)

### Output structure

Each paper gets a named folder for easy search by author/keyword across a literature library:

```
output_dir/
├── wei2022_Chain-of-thought_prompting.../
│   ├── wei2022_Chain-of-thought_prompting....md           ← paper content
│   ├── wei2022_Chain-of-thought_prompting..._content.json ← structured TOC with semantic types
│   └── images/                                             ← extracted figures/tables
```

- **`{stem}.md`** — full paper as markdown (essential, always present)
- **`{stem}_content.json`** — structured content list with element types (title, paragraph, table, figure) and bounding boxes; useful for AI agents to quickly locate sections/figures without scanning full markdown
- **`images/`** — extracted figures and tables referenced by the markdown

MinerU names files inside the zip `<task-uuid>_<name>` (`<uuid>_content_list_v2.json`) — finders match on suffix (fixed 1.2.0; before that `_content.json` was silently skipped).

Naming uses `author_year_title` convention from the original filename, with spaces → underscores, special chars sanitized, max 128 chars.

## Development Notes

- Presigned OSS URLs are signed WITHOUT Content-Type — using axios for upload would fail because axios force-adds the header. Native `fetch` is used instead.
- `data_id` preserves original filename (spaces → underscores, special chars sanitized, max 128 chars) with collision detection.
- Smithery integration: `createServer()` export for hosted deployment, `createSandboxServer()` for scanning.
- Dual entry: `index.ts` = stdio transport (MCP clients), `server.ts` = HTTP/Express transport.

## Limits

- Single file: 200MB max, 200 pages max (use `pages` to parse a longer file in ≤200-page slices — verified 2026-09-16)
- Daily quota: 1000 pages at high priority (excess is deprioritized, not rejected)
- Batch: max 200 files per request
- Models: `pipeline` (fast) or `vlm` (90% accuracy, recommended for academic PDFs)

## Release (tokenless OIDC)

CI (`.github/workflows/publish-mcp.yml`) publishes on a `v*` tag — to npm (OIDC Trusted Publishing) **and** the MCP Registry (`mcp-publisher login github-oidc`, namespace `io.github.linxule/mineru`). Tokenless; no manual `npm publish` / `mcp-publisher`. Bun toolchain (`bun.lock`), grouped Dependabot. Since v1.1.4.

1. Bump `version` in **`package.json` AND `server.json`** (both top-level `version` and `packages[0].version`) — npm + Registry reject duplicate versions.
2. `bun run build` (tsc)
3. Commit + push (PRs run the build gate)
4. `git tag vX.Y.Z && git push origin vX.Y.Z` → CI publishes npm then the Registry.
5. **If the run fails at "Wait for exact npm version to propagate"** (npm took ~9 min on
   2026-09-16; the wait gives up at 5), the npm publish already succeeded — do not re-tag.
   Once `curl -sf https://registry.npmjs.org/mineru-mcp/X.Y.Z` returns, run the recovery path:
   `gh workflow run publish-mcp.yml --ref vX.Y.Z -f registry_tag=vX.Y.Z` (skips npm, publishes
   the Registry).

**One-time setup (done 2026-06-22):** npm Trusted Publisher for `mineru-mcp` (owner `linxule`, repo, workflow `publish-mcp.yml`, Environment blank; 2FA mode = "2FA **or** automation tokens"). Migrated npm→bun at v1.1.4 (the old `package-lock.json` was stale).
