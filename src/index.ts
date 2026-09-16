#!/usr/bin/env node
import { VERSION } from "./version.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import axios, { AxiosError } from "axios";
import { readFileSync, createWriteStream, readdirSync, statSync, mkdirSync, existsSync, unlinkSync, rmSync, realpathSync, copyFileSync } from "node:fs";
import { join, basename, extname } from "node:path";
import { execFileSync } from "node:child_process";
import { tmpdir } from "node:os";
import { randomBytes } from "node:crypto";
import { pipeline } from "node:stream/promises";

// Configuration schema for Smithery
export const configSchema = z.object({
  mineruApiKey: z.string().describe("MinerU API key from mineru.net"),
  mineruBaseUrl: z
    .string()
    .optional()
    .default("https://mineru.net/api/v4")
    .describe("API base URL"),
  mineruDefaultModel: z
    .enum(["pipeline", "vlm"])
    .optional()
    .default("pipeline")
    .describe("Default model: pipeline (fast) or vlm (90% accuracy)"),
});

type Config = z.infer<typeof configSchema>;

// Error codes with actionable messages
const ERROR_MESSAGES: Record<string, string> = {
  A0202: "Token error. Check your API key.",
  A0211: "Token expired. Get a new API key.",
  "-60002": "Invalid file format. Use: pdf, doc, docx, ppt, pptx, png, jpg, jpeg",
  "-60005": "File too large. Max 200MB.",
  "-60006": "Too many pages. Max 200 per file. Re-submit with pages (e.g. 1-200, 201-400) — ranges are accepted on files longer than 200 pages.",
  "-60008": "URL timeout. Check the URL is accessible.",
  "-60009": "Queue full. Try again later.",
  "-60012": "Task not found. Check task_id is valid.",
  "-60013": "Access denied. You can only access your own tasks.",
};

// Response types
interface TaskResponse {
  task_id: string;
}

interface TaskStatus {
  task_id: string;
  data_id?: string;
  state: "pending" | "running" | "done" | "failed" | "converting";
  full_zip_url?: string;
  err_msg?: string;
  extract_progress?: {
    extracted_pages: number;
    total_pages: number;
    start_time: string;
  };
}

interface BatchResponse {
  batch_id: string;
}

interface BatchFileUploadResponse {
  batch_id: string;
  file_urls: string[];
}

interface BatchStatus {
  batch_id: string;
  extract_result: Array<{
    file_name: string;
    state: string;
    full_zip_url?: string;
    err_msg?: string;
    data_id?: string;
    extract_progress?: {
      extracted_pages: number;
      total_pages: number;
      start_time: string;
    };
  }>;
}

// Long-document slicing (MinerU rejects files >200 pages, but accepts page_ranges on them)
const MAX_SLICE_PAGES = 200;

function planSlices(totalPages: number, sliceSize: number): Array<[number, number]> {
  if (!Number.isInteger(totalPages) || totalPages < 1) throw new Error("total_pages must be a positive integer");
  const size = Math.min(Math.max(1, Math.floor(sliceSize)), MAX_SLICE_PAGES);
  const slices: Array<[number, number]> = [];
  for (let start = 1; start <= totalPages; start += size) {
    slices.push([start, Math.min(start + size - 1, totalPages)]);
  }
  return slices;
}

// data_id must be [A-Za-z0-9_.-], ≤128 chars. Encode the slice so merge can order it.
function sliceDataId(name: string, start: number, end: number): string {
  const stem = name.replace(/[^a-zA-Z0-9_\-\.]/g, "_").slice(0, 100) || "document";
  return `${stem}__p${String(start).padStart(5, "0")}-${String(end).padStart(5, "0")}`;
}

function parseSliceId(dataId: string | undefined): { name: string; start: number; end: number } | null {
  const m = dataId?.match(/^(.+)__p(\d{5})-(\d{5})$/);
  return m ? { name: m[1], start: Number(m[2]), end: Number(m[3]) } : null;
}

// Depth-limited, symlink-safe finders (zip-slip protection)
function findEntry(dir: string, targetName: string, baseDir: string, wantDir: boolean, depth = 0): string | null {
  if (depth > 5) return null;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.isSymbolicLink()) continue;
    const fullPath = join(dir, entry.name);
    // MinerU prefixes files with the task uuid ("<uuid>_content_list_v2.json"), so match on suffix
    const matches = (entry.name === targetName || entry.name.endsWith(`_${targetName}`)) && (wantDir ? entry.isDirectory() : entry.isFile());
    if (matches && realpathSync(fullPath).startsWith(realpathSync(baseDir))) return fullPath;
    if (entry.isDirectory()) {
      const found = findEntry(fullPath, targetName, baseDir, wantDir, depth + 1);
      if (found) return found;
    }
  }
  return null;
}

async function downloadAndUnzip(zipUrl: string, tmpBase: string, stem: string): Promise<string> {
  const zipPath = join(tmpBase, `${stem}.zip`);
  // The CDN sometimes drops the first connection right after a result is published ("aborted"); retry once.
  for (let attempt = 1; ; attempt++) {
    try {
      const response = await axios.get(zipUrl, { responseType: "stream", timeout: 120_000 });
      await pipeline(response.data, createWriteStream(zipPath));
      break;
    } catch (err) {
      if (attempt >= 3) throw err;
      await new Promise((r) => setTimeout(r, 2_000 * attempt));
    }
  }
  const extractDir = join(tmpBase, stem);
  mkdirSync(extractDir, { recursive: true });
  execFileSync("unzip", ["-o", "-q", zipPath, "-d", extractDir], { timeout: 60_000 });
  unlinkSync(zipPath);
  return extractDir;
}

// Format helpers
function formatConciseStatus(status: TaskStatus): string {
  const parts = [status.state, status.task_id];
  if (status.state === "done" && status.full_zip_url) {
    parts.push(status.full_zip_url);
  } else if (status.state === "running" && status.extract_progress) {
    const p = status.extract_progress;
    parts.push(`${p.extracted_pages}/${p.total_pages} pages`);
  } else if (status.state === "failed" && status.err_msg) {
    parts.push(status.err_msg);
  }
  return parts.join(" | ");
}

function formatDetailedStatus(status: TaskStatus): string {
  return JSON.stringify(status, null, 2);
}

function formatConciseBatch(batch: BatchStatus, limit: number, offset: number): string {
  const results = batch.extract_result.slice(offset, offset + limit);
  const total = batch.extract_result.length;
  const done = batch.extract_result.filter((r) => r.state === "done").length;

  const lines = [`Batch ${batch.batch_id}: ${done}/${total} done`];
  for (const r of results) {
    let line = `- ${r.file_name}: ${r.state}`;
    if (r.state === "done" && r.full_zip_url) {
      line += ` ${r.full_zip_url}`;
    } else if (r.state === "running" && r.extract_progress) {
      line += ` (${r.extract_progress.extracted_pages}/${r.extract_progress.total_pages})`;
    }
    lines.push(line);
  }

  if (offset + limit < total) {
    lines.push(`[+${total - offset - limit} more, use offset=${offset + limit}]`);
  }

  return lines.join("\n");
}

// Create server function for Smithery
export default function createServer({ config }: { config: Config }) {
  const apiKey = config.mineruApiKey;
  const baseUrl = config.mineruBaseUrl || "https://mineru.net/api/v4";
  const defaultModel = config.mineruDefaultModel || "pipeline";

  // API client with injected config
  async function mineruRequest<T>(
    endpoint: string,
    method: "GET" | "POST" = "GET",
    data?: unknown
  ): Promise<T> {
    if (!apiKey) {
      throw new Error("MINERU_API_KEY not set. Add it to your environment.");
    }

    try {
      const response = await axios({
        method,
        url: `${baseUrl}${endpoint}`,
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        data,
      });

      const result = response.data;
      if (result.code !== 0) {
        const code = String(result.code);
        const msg = ERROR_MESSAGES[code] || result.msg || "Unknown error";
        throw new Error(`MinerU error ${code}: ${msg}`);
      }

      return result.data as T;
    } catch (error) {
      if (error instanceof AxiosError) {
        const code = error.response?.data?.code;
        if (code) {
          const msg = ERROR_MESSAGES[String(code)] || error.response?.data?.msg;
          throw new Error(`MinerU error ${code}: ${msg}`);
        }
        throw new Error(`HTTP ${error.response?.status}: ${error.message}`);
      }
      throw error;
    }
  }

  // Create MCP server
  const server = new McpServer({
    name: "mineru",
    version: VERSION,
  });

  // Tool 1: mineru_parse
  server.tool(
    "mineru_parse",
    "Parse a document URL. Returns task_id to check status.",
    {
      url: z.string().describe("Document URL (PDF, DOC, PPT, images)"),
      model: z
        .enum(["pipeline", "vlm"])
        .optional()
        .describe("pipeline=fast, vlm=90% accuracy"),
      pages: z.string().optional().describe("Page range: 1-10,15 or 2--2"),
      ocr: z.boolean().optional().describe("Enable OCR (pipeline only)"),
      formula: z.boolean().optional().describe("Formula recognition"),
      table: z.boolean().optional().describe("Table recognition"),
      language: z.string().optional().describe("Language code: ch, en, etc"),
      formats: z
        .array(z.enum(["docx", "html", "latex"]))
        .optional()
        .describe("Extra export formats"),
    },
    async (params) => {
      const requestData: Record<string, unknown> = {
        url: params.url,
        model_version: params.model || defaultModel,
      };

      if (params.pages) requestData.page_ranges = params.pages;
      if (params.ocr !== undefined) requestData.is_ocr = params.ocr;
      if (params.formula !== undefined) requestData.enable_formula = params.formula;
      if (params.table !== undefined) requestData.enable_table = params.table;
      if (params.language) requestData.language = params.language;
      if (params.formats?.length) requestData.extra_formats = params.formats;

      const result = await mineruRequest<TaskResponse>("/extract/task", "POST", requestData);

      return {
        content: [
          {
            type: "text",
            text: `Task created: ${result.task_id}\nUse mineru_status to check progress.`,
          },
        ],
      };
    }
  );

  // Tool 2: mineru_status
  server.tool(
    "mineru_status",
    "Check task progress. Returns download URL when done.",
    {
      task_id: z.string().describe("Task ID from mineru_parse"),
      format: z
        .enum(["concise", "detailed"])
        .optional()
        .default("concise")
        .describe("Output format"),
    },
    async (params) => {
      const status = await mineruRequest<TaskStatus>(`/extract/task/${params.task_id}`);

      const text =
        params.format === "detailed"
          ? formatDetailedStatus(status)
          : formatConciseStatus(status);

      return {
        content: [{ type: "text", text }],
      };
    }
  );

  // Tool 3: mineru_batch
  server.tool(
    "mineru_batch",
    "Parse multiple URLs in one batch (max 200). Preferred over mineru_upload_batch — faster and more reliable. Use public URLs (arXiv, SSRN, publisher sites) when available.",
    {
      urls: z.union([z.array(z.string()), z.string()]).describe("Array of document URLs, or a single URL string"),
      model: z
        .enum(["pipeline", "vlm"])
        .optional()
        .describe("pipeline=fast, vlm=90% accuracy"),
      ocr: z.boolean().optional().describe("Enable OCR (pipeline only)"),
      formula: z.boolean().optional().describe("Formula recognition"),
      table: z.boolean().optional().describe("Table recognition"),
      language: z.string().optional().describe("Language code: ch, en, etc"),
      formats: z
        .array(z.enum(["docx", "html", "latex"]))
        .optional()
        .describe("Extra export formats"),
    },
    async (params) => {
      // Normalize urls: accept string (JSON array or single URL) or array
      let urls: string[];
      if (typeof params.urls === "string") {
        try {
          const parsed = JSON.parse(params.urls);
          urls = Array.isArray(parsed) ? parsed : [params.urls];
        } catch {
          urls = [params.urls];
        }
      } else {
        urls = params.urls;
      }

      if (urls.length > 200) {
        throw new Error("Max 200 URLs per batch. Split into smaller batches.");
      }

      const requestData: Record<string, unknown> = {
        files: urls.map((url) => ({ url })),
        model_version: params.model || defaultModel,
      };

      if (params.ocr !== undefined) requestData.is_ocr = params.ocr;
      if (params.formula !== undefined) requestData.enable_formula = params.formula;
      if (params.table !== undefined) requestData.enable_table = params.table;
      if (params.language) requestData.language = params.language;
      if (params.formats?.length) requestData.extra_formats = params.formats;

      const result = await mineruRequest<BatchResponse>("/extract/task/batch", "POST", requestData);

      return {
        content: [
          {
            type: "text",
            text: `Batch created: ${result.batch_id}\n${urls.length} files queued.\nUse mineru_batch_status to check progress.`,
          },
        ],
      };
    }
  );

  // Tool 4: mineru_batch_status
  server.tool(
    "mineru_batch_status",
    "Get batch results. Supports pagination for large batches.",
    {
      batch_id: z.string().describe("Batch ID from mineru_batch"),
      limit: z.number().optional().default(10).describe("Max results to return"),
      offset: z.number().optional().default(0).describe("Skip first N results"),
      format: z
        .enum(["concise", "detailed"])
        .optional()
        .default("concise")
        .describe("Output format"),
    },
    async (params) => {
      const batch = await mineruRequest<BatchStatus>(
        `/extract-results/batch/${params.batch_id}`
      );

      const text =
        params.format === "detailed"
          ? JSON.stringify(batch, null, 2)
          : formatConciseBatch(batch, params.limit ?? 10, params.offset ?? 0);

      return {
        content: [{ type: "text", text }],
      };
    }
  );

  // Tool 5: mineru_upload_batch
  server.tool(
    "mineru_upload_batch",
    "Upload local files for batch parsing. SLOW: uploads can take minutes and may timeout. Prefer mineru_batch with public URLs (arXiv, SSRN, publisher sites) when available — it's faster and more reliable. Only use this for files not available online.",
    {
      directory: z.string().optional().describe("Directory path containing PDF/DOC/PPT files"),
      files: z.union([z.array(z.string()), z.string()]).optional().describe("Array of absolute file paths, or a single path string"),
      model: z
        .enum(["pipeline", "vlm"])
        .optional()
        .describe("pipeline=fast, vlm=90% accuracy"),
      formula: z.boolean().optional().describe("Formula recognition"),
      table: z.boolean().optional().describe("Table recognition"),
      language: z.string().optional().describe("Language code: ch, en, etc"),
      formats: z
        .array(z.enum(["docx", "html", "latex"]))
        .optional()
        .describe("Extra export formats"),
    },
    async (params) => {
      const supportedExts = new Set([".pdf", ".doc", ".docx", ".ppt", ".pptx", ".png", ".jpg", ".jpeg"]);

      // Collect files — normalize string input (JSON array or single path)
      let filePaths: string[] = [];
      if (params.files) {
        if (typeof params.files === "string") {
          try {
            const parsed = JSON.parse(params.files);
            filePaths = Array.isArray(parsed) ? parsed : [params.files];
          } catch {
            filePaths = [params.files];
          }
        } else {
          filePaths = params.files;
        }
      } else if (params.directory) {
        const dir = params.directory;
        if (!existsSync(dir)) {
          throw new Error(`Directory not found: ${dir}`);
        }
        const entries = readdirSync(dir);
        filePaths = entries
          .filter((f) => supportedExts.has(extname(f).toLowerCase()))
          .map((f) => join(dir, f));
      } else {
        throw new Error("Provide either 'directory' or 'files' parameter.");
      }

      if (filePaths.length === 0) {
        throw new Error("No supported files found.");
      }
      if (filePaths.length > 200) {
        throw new Error(`Found ${filePaths.length} files. Max 200 per batch. Filter or split.`);
      }

      // Validate files exist and build request with collision-safe data_ids
      const fileEntries: Array<{ name: string; data_id: string }> = [];
      const fileSizes: number[] = [];
      const usedDataIds = new Set<string>();
      for (const fp of filePaths) {
        if (!existsSync(fp)) {
          throw new Error(`File not found: ${fp}`);
        }
        const stats = statSync(fp);
        fileSizes.push(stats.size);
        if (stats.size > 200 * 1024 * 1024) {
          throw new Error(`File too large (${(stats.size / 1024 / 1024).toFixed(0)}MB): ${basename(fp)}. Max 200MB.`);
        }
        const name = basename(fp);
        let stem = name.replace(extname(name), "").replace(/[^a-zA-Z0-9_\-\.]/g, "_").slice(0, 128);
        // Handle data_id collisions
        let candidate = stem;
        let counter = 1;
        while (usedDataIds.has(candidate)) {
          candidate = `${stem}_${counter++}`;
        }
        usedDataIds.add(candidate);
        fileEntries.push({ name, data_id: candidate });
      }

      // Request upload URLs
      const requestData: Record<string, unknown> = {
        files: fileEntries,
        model_version: params.model || defaultModel,
      };
      if (params.formula !== undefined) requestData.enable_formula = params.formula;
      if (params.table !== undefined) requestData.enable_table = params.table;
      if (params.language) requestData.language = params.language;
      if (params.formats?.length) requestData.extra_formats = params.formats;

      const result = await mineruRequest<BatchFileUploadResponse>("/file-urls/batch", "POST", requestData);

      if (result.file_urls.length !== filePaths.length) {
        throw new Error(`Expected ${filePaths.length} upload URLs, got ${result.file_urls.length}`);
      }

      // Upload each file to presigned OSS URLs using native fetch
      // Presigned URLs are signed WITHOUT Content-Type — axios force-adds it, so use fetch
      // Size-proportional timeout: 60s base + 2s per MB (fail fast for small files, generous for large)
      const uploadResults: string[] = [];
      for (let i = 0; i < filePaths.length; i++) {
        const fp = filePaths[i];
        const uploadUrl = result.file_urls[i];
        const fileName = basename(fp);
        const sizeMB = (fileSizes[i] / 1024 / 1024).toFixed(1);
        const timeoutMs = Math.max(60_000, 60_000 + Math.ceil(fileSizes[i] / (1024 * 1024)) * 2_000);
        try {
          const fileData = readFileSync(fp);
          const resp = await fetch(uploadUrl, {
            method: "PUT",
            body: fileData,
            signal: AbortSignal.timeout(timeoutMs),
          });
          if (!resp.ok) {
            const body = await resp.text();
            uploadResults.push(`FAIL: ${fileName} (${sizeMB}MB) - HTTP ${resp.status}: ${body.slice(0, 200)}`);
          } else {
            uploadResults.push(`OK: ${fileName} (${sizeMB}MB)`);
          }
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          const isTimeout = err instanceof DOMException && err.name === "TimeoutError";
          uploadResults.push(`FAIL: ${fileName} (${sizeMB}MB) - ${isTimeout ? `TIMEOUT after ${Math.round(timeoutMs / 1000)}s` : msg}`);
        }
      }

      const successCount = uploadResults.filter((r) => r.startsWith("OK")).length;
      const failCount = uploadResults.filter((r) => r.startsWith("FAIL")).length;
      const timeoutCount = uploadResults.filter((r) => r.includes("TIMEOUT")).length;

      let text = `Batch ${result.batch_id}: ${successCount} uploaded, ${failCount} failed.\n`;
      if (successCount > 0) {
        text += `Parsing starts automatically. Use mineru_batch_status to track.\n`;
      }
      if (failCount > 0) {
        text += `\nFailed uploads:\n${uploadResults.filter((r) => r.startsWith("FAIL")).join("\n")}`;
      }
      if (timeoutCount > 0) {
        text += `\n\nTIP: Upload timed out. Try mineru_batch with public URLs instead (arXiv, SSRN, publisher sites) — it's faster and more reliable.`;
      }

      return {
        content: [{ type: "text", text }],
      };
    }
  );

  // Tool 6: mineru_download_results
  server.tool(
    "mineru_download_results",
    "Download batch results and extract named paper folders. Each folder contains {name}.md, {name}_content.json (structured TOC), and images/. Output includes parsed title — verify it matches the expected paper.",
    {
      batch_id: z.string().describe("Batch ID from mineru_upload_batch or mineru_batch"),
      output_dir: z.string().describe("Directory to save markdown files"),
      overwrite: z.boolean().optional().default(false).describe("Overwrite existing files"),
    },
    async (params) => {
      // Check batch status
      const batch = await mineruRequest<BatchStatus>(
        `/extract-results/batch/${params.batch_id}`
      );

      const results = batch.extract_result;
      const doneResults = results.filter((r) => r.state === "done" && r.full_zip_url);
      const pendingResults = results.filter((r) => ["pending", "running", "converting"].includes(r.state));
      const failedResults = results.filter((r) => r.state === "failed");

      if (doneResults.length === 0 && pendingResults.length > 0) {
        return {
          content: [{
            type: "text",
            text: `Batch ${params.batch_id}: ${pendingResults.length} still processing, 0 done. Try again later.`,
          }],
        };
      }

      // Create output directory
      mkdirSync(params.output_dir, { recursive: true });

      const tmpBase = join(tmpdir(), `mineru-dl-${Date.now()}-${randomBytes(4).toString("hex")}`);
      mkdirSync(tmpBase, { recursive: true });

      const downloaded: string[] = [];
      const errors: string[] = [];

      // Depth-limited, symlink-safe file finder
      const findFile = (dir: string, targetName: string, baseDir: string, depth = 0, maxDepth = 5): string | null => {
        if (depth > maxDepth) return null;
        const entries = readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.isSymbolicLink()) continue; // skip symlinks (zip slip protection)
          const fullPath = join(dir, entry.name);
          if (entry.isFile() && (entry.name === targetName || entry.name.endsWith(`_${targetName}`))) {
            const realPath = realpathSync(fullPath);
            if (!realPath.startsWith(realpathSync(baseDir))) continue;
            return fullPath;
          }
          if (entry.isDirectory()) {
            const found = findFile(fullPath, targetName, baseDir, depth + 1, maxDepth);
            if (found) return found;
          }
        }
        return null;
      };

      // Depth-limited, symlink-safe directory finder
      const findDir = (dir: string, targetName: string, baseDir: string, depth = 0, maxDepth = 5): string | null => {
        if (depth > maxDepth) return null;
        const entries = readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.isSymbolicLink()) continue;
          const fullPath = join(dir, entry.name);
          if (entry.isDirectory() && entry.name === targetName) {
            const realPath = realpathSync(fullPath);
            if (!realPath.startsWith(realpathSync(baseDir))) continue;
            return fullPath;
          }
          if (entry.isDirectory()) {
            const found = findDir(fullPath, targetName, baseDir, depth + 1, maxDepth);
            if (found) return found;
          }
        }
        return null;
      };

      for (const r of doneResults) {
        // Prefer data_id (set by us from original filename) over file_name (API-returned, can be stale)
        const rawName = r.data_id || r.file_name || "unknown";
        const safeName = basename(rawName).replace(/[^a-zA-Z0-9_\-\.]/g, "_");
        const stem = (safeName.replace(extname(safeName), "") || "unnamed").slice(0, 128);
        const paperDir = join(params.output_dir, stem);

        if (existsSync(paperDir)) {
          if (!params.overwrite) {
            downloaded.push(`SKIP: ${stem}/ (exists)`);
            continue;
          }
          // Clean existing folder to avoid stale files from previous download
          rmSync(paperDir, { recursive: true, force: true });
        }

        try {
          // Download zip via streaming to avoid memory pressure
          const zipPath = join(tmpBase, `${stem}.zip`);
          const response = await axios.get(r.full_zip_url!, {
            responseType: "stream",
            timeout: 120_000,
          });
          await pipeline(response.data, createWriteStream(zipPath));

          // Extract zip using execFileSync (no shell injection)
          const extractDir = join(tmpBase, stem);
          mkdirSync(extractDir, { recursive: true });
          try {
            execFileSync("unzip", ["-o", "-q", zipPath, "-d", extractDir], {
              timeout: 60_000,
            });
          } catch (unzipErr) {
            const msg = unzipErr instanceof Error ? unzipErr.message : String(unzipErr);
            errors.push(`UNZIP_FAIL: ${safeName} - ${msg}`);
            continue;
          }

          // Find essential files in extracted zip
          const mdFile = findFile(extractDir, "full.md", extractDir);
          if (!mdFile) {
            errors.push(`NO_MD: ${safeName} - no full.md found in zip`);
            continue;
          }

          // Create paper folder and copy essential files with named prefixes
          mkdirSync(paperDir, { recursive: true });

          // 1. Markdown (essential)
          copyFileSync(mdFile, join(paperDir, `${stem}.md`));

          // Extract title from first heading for verification
          const mdHead = readFileSync(mdFile, "utf-8").slice(0, 500);
          const titleMatch = mdHead.match(/^#\s+(.+)/m);
          const parsedTitle = titleMatch ? titleMatch[1].trim().slice(0, 120) : null;

          // 2. Structured content list (useful for AI navigation)
          const contentFile = findFile(extractDir, "content_list_v2.json", extractDir);
          if (contentFile) {
            copyFileSync(contentFile, join(paperDir, `${stem}_content.json`));
          }

          // 3. Images directory (figures/tables referenced by markdown)
          // Manual copy to skip symlinks (cpSync follows them, bypassing zip-slip protection)
          const imagesDir = findDir(extractDir, "images", extractDir);
          if (imagesDir) {
            const destImages = join(paperDir, "images");
            mkdirSync(destImages, { recursive: true });
            for (const entry of readdirSync(imagesDir, { withFileTypes: true })) {
              if (entry.isSymbolicLink()) continue;
              if (entry.isFile()) {
                copyFileSync(join(imagesDir, entry.name), join(destImages, entry.name));
              }
            }
          }

          downloaded.push(parsedTitle ? `OK: ${stem}/ — "${parsedTitle}"` : `OK: ${stem}/`);

          // Cleanup zip
          unlinkSync(zipPath);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          errors.push(`FAIL: ${safeName} - ${msg}`);
        }
      }

      // Cleanup temp directory
      try {
        rmSync(tmpBase, { recursive: true, force: true });
      } catch { /* ignore cleanup errors */ }

      let text = `Downloaded to: ${params.output_dir}\n`;
      text += `Done: ${downloaded.filter((d) => d.startsWith("OK")).length}`;
      text += ` | Skipped: ${downloaded.filter((d) => d.startsWith("SKIP")).length}`;
      text += ` | Failed: ${errors.length}`;
      if (pendingResults.length > 0) {
        text += ` | Still processing: ${pendingResults.length}`;
      }
      if (failedResults.length > 0) {
        text += ` | Parse failed: ${failedResults.length}`;
      }
      text += `\n\nFiles:\n${downloaded.join("\n")}`;
      if (errors.length > 0) {
        text += `\n\nErrors:\n${errors.join("\n")}`;
      }
      if (pendingResults.length > 0) {
        text += `\n\nRe-run this tool to download remaining files once processing completes.`;
      }

      return {
        content: [{ type: "text", text }],
      };
    }
  );

  // Tool 7: mineru_parse_long — one document > 200 pages, submitted as ≤200-page slices in one batch
  server.tool(
    "mineru_parse_long",
    "Parse a document LONGER than 200 pages (MinerU's per-file cap) by submitting it as one batch of ≤200-page slices with page_ranges. Give total_pages (from `mdls -name kMDItemNumberOfPages`, `pdfinfo`, or the viewer) — it is auto-detected only for local files on macOS. Returns a batch_id; poll with mineru_batch_status, then stitch with mineru_merge_slices. Files ≤200 pages: use mineru_parse instead.",
    {
      url: z.string().optional().describe("Public document URL (preferred)"),
      file: z.string().optional().describe("Absolute local file path (uploaded once per slice — slow for big files)"),
      total_pages: z.number().int().positive().optional().describe("Total page count of the document"),
      slice_size: z.number().int().positive().max(MAX_SLICE_PAGES).optional().default(MAX_SLICE_PAGES).describe("Pages per slice (≤200)"),
      name: z.string().optional().describe("Output name for the merged result (default: from URL/file name)"),
      model: z.enum(["pipeline", "vlm"]).optional().describe("pipeline=fast, vlm=90% accuracy"),
      ocr: z.boolean().optional().describe("Enable OCR (pipeline only)"),
      formula: z.boolean().optional().describe("Formula recognition"),
      table: z.boolean().optional().describe("Table recognition"),
      language: z.string().optional().describe("Language code: ch, en, etc"),
    },
    async (params) => {
      if (!params.url === !params.file) throw new Error("Provide exactly one of 'url' or 'file'.");

      let totalPages = params.total_pages;
      if (!totalPages && params.file && process.platform === "darwin") {
        try {
          const out = execFileSync("mdls", ["-raw", "-name", "kMDItemNumberOfPages", params.file], { timeout: 10_000 }).toString().trim();
          if (/^\d+$/.test(out)) totalPages = Number(out);
        } catch { /* fall through to the error below */ }
      }
      if (!totalPages) throw new Error("total_pages is required (could not auto-detect). Get it with `mdls -name kMDItemNumberOfPages <file>` or `pdfinfo`.");

      const source = params.url || params.file!;
      const rawName = params.name || basename(new URL(params.url || `file://${params.file}`).pathname);
      const name = rawName.replace(extname(rawName), "") || "document";
      const slices = planSlices(totalPages, params.slice_size);
      if (slices.length > 200) throw new Error(`${slices.length} slices exceeds the 200-file batch limit; raise slice_size.`);

      const common: Record<string, unknown> = { model_version: params.model || defaultModel };
      if (params.formula !== undefined) common.enable_formula = params.formula;
      if (params.table !== undefined) common.enable_table = params.table;
      if (params.language) common.language = params.language;

      const entries = slices.map(([a, b]) => {
        const e: Record<string, unknown> = { data_id: sliceDataId(name, a, b), page_ranges: `${a}-${b}` };
        if (params.ocr !== undefined) e.is_ocr = params.ocr;
        return e;
      });

      let batchId: string;
      const uploadNotes: string[] = [];
      if (params.url) {
        const result = await mineruRequest<BatchResponse>("/extract/task/batch", "POST", {
          ...common,
          files: entries.map((e) => ({ ...e, url: params.url })),
        });
        batchId = result.batch_id;
      } else {
        if (!existsSync(params.file!)) throw new Error(`File not found: ${params.file}`);
        const size = statSync(params.file!).size;
        if (size > 200 * 1024 * 1024) throw new Error(`File too large (${(size / 1024 / 1024).toFixed(0)}MB). Max 200MB.`);
        const result = await mineruRequest<BatchFileUploadResponse>("/file-urls/batch", "POST", {
          ...common,
          files: entries.map((e) => ({ ...e, name: basename(params.file!) })),
        });
        if (result.file_urls.length !== entries.length) throw new Error(`Expected ${entries.length} upload URLs, got ${result.file_urls.length}`);
        batchId = result.batch_id;
        const data = readFileSync(params.file!);
        const timeoutMs = 60_000 + Math.ceil(size / (1024 * 1024)) * 2_000;
        for (let i = 0; i < result.file_urls.length; i++) {
          try {
            const resp = await fetch(result.file_urls[i], { method: "PUT", body: data, signal: AbortSignal.timeout(timeoutMs) });
            if (!resp.ok) uploadNotes.push(`FAIL slice ${slices[i][0]}-${slices[i][1]}: HTTP ${resp.status}`);
          } catch (err) {
            uploadNotes.push(`FAIL slice ${slices[i][0]}-${slices[i][1]}: ${err instanceof Error ? err.message : String(err)}`);
          }
        }
      }

      let text = `Batch ${batchId}: "${name}" (${totalPages} pages) queued as ${slices.length} slice(s) of ≤${params.slice_size} pages from ${source}.\n`;
      text += slices.map(([a, b]) => `  ${sliceDataId(name, a, b)}  pages ${a}-${b}`).join("\n");
      text += `\nPoll with mineru_batch_status, then mineru_merge_slices(batch_id, output_dir).`;
      if (uploadNotes.length) text += `\n\nUpload problems:\n${uploadNotes.join("\n")}`;
      return { content: [{ type: "text", text }] };
    }
  );

  // Tool 8: mineru_merge_slices — stitch a sliced batch back into one document
  server.tool(
    "mineru_merge_slices",
    "Stitch the slices of a mineru_parse_long batch into one {name}/{name}.md (+ {name}_content.json with page_idx re-based to the whole document, + images/). Slices are ordered by their page range; each is marked with an HTML comment. Waits for nothing — if any slice is still processing, it reports and you re-run later.",
    {
      batch_id: z.string().describe("Batch ID from mineru_parse_long"),
      output_dir: z.string().describe("Directory to write the merged document folder into"),
      overwrite: z.boolean().optional().default(false).describe("Overwrite an existing merged folder"),
    },
    async (params) => {
      const batch = await mineruRequest<BatchStatus>(`/extract-results/batch/${params.batch_id}`);
      const slices = batch.extract_result
        .map((r) => ({ r, s: parseSliceId(r.data_id) }))
        .filter((x): x is { r: BatchStatus["extract_result"][number]; s: NonNullable<ReturnType<typeof parseSliceId>> } => x.s !== null)
        .sort((a, b) => a.s.start - b.s.start);
      if (slices.length === 0) throw new Error("No slice entries in this batch (data_id must look like name__p00001-00200). Was it created by mineru_parse_long?");

      const pending = slices.filter((x) => ["pending", "running", "converting"].includes(x.r.state));
      const failed = slices.filter((x) => x.r.state === "failed");
      if (pending.length || failed.length) {
        let text = `Batch ${params.batch_id}: ${slices.length - pending.length - failed.length}/${slices.length} slices done.`;
        if (pending.length) text += `\nStill processing: ${pending.map((x) => `${x.s.start}-${x.s.end}`).join(", ")}`;
        if (failed.length) text += `\nFailed: ${failed.map((x) => `${x.s.start}-${x.s.end} (${x.r.err_msg || "no message"})`).join("; ")}\nRe-submit failed ranges with mineru_parse(pages=...) or fix and re-run mineru_parse_long.`;
        if (pending.length) text += `\nRe-run mineru_merge_slices when all slices are done.`;
        return { content: [{ type: "text", text }] };
      }

      const name = slices[0].s.name;
      const outDir = join(params.output_dir, name);
      if (existsSync(outDir)) {
        if (!params.overwrite) throw new Error(`${outDir} exists. Pass overwrite=true to replace it.`);
        rmSync(outDir, { recursive: true, force: true });
      }
      const imagesOut = join(outDir, "images");
      mkdirSync(imagesOut, { recursive: true });

      const tmpBase = join(tmpdir(), `mineru-merge-${Date.now()}-${randomBytes(4).toString("hex")}`);
      mkdirSync(tmpBase, { recursive: true });

      const mdParts: string[] = [];
      const contentList: unknown[] = [];
      const notes: string[] = [];
      let imageCount = 0;
      try {
        for (const { r, s } of slices) {
          const tag = `p${String(s.start).padStart(5, "0")}-${String(s.end).padStart(5, "0")}`;
          const extractDir = await downloadAndUnzip(r.full_zip_url!, tmpBase, tag);

          const mdFile = findEntry(extractDir, "full.md", extractDir, false);
          if (!mdFile) { notes.push(`slice ${s.start}-${s.end}: no full.md`); continue; }
          // Prefix image refs so slices can't collide on MinerU's per-zip image names
          const md = readFileSync(mdFile, "utf-8").replace(/\]\(images\//g, `](images/${tag}_`);
          mdParts.push(`<!-- mineru slice: pages ${s.start}-${s.end} -->\n\n${md.trim()}\n`);

          const imagesDir = findEntry(extractDir, "images", extractDir, true);
          if (imagesDir) {
            for (const entry of readdirSync(imagesDir, { withFileTypes: true })) {
              if (entry.isFile() && !entry.isSymbolicLink()) {
                copyFileSync(join(imagesDir, entry.name), join(imagesOut, `${tag}_${entry.name}`));
                imageCount++;
              }
            }
          }

          const contentFile = findEntry(extractDir, "content_list_v2.json", extractDir, false)
            || findEntry(extractDir, "content_list.json", extractDir, false);
          if (contentFile) {
            try {
              const items = JSON.parse(readFileSync(contentFile, "utf-8"));
              const offset = s.start - 1; // slice page_idx is 0-based within the slice
              const rebase = (item: unknown): unknown => {
                if (Array.isArray(item)) return item.map(rebase);
                if (item && typeof item === "object") {
                  const o = { ...(item as Record<string, unknown>) };
                  if (typeof o.page_idx === "number") o.page_idx = o.page_idx + offset;
                  if (typeof o.img_path === "string") o.img_path = o.img_path.replace(/^images\//, `images/${tag}_`);
                  for (const k of Object.keys(o)) if (k !== "page_idx") o[k] = rebase(o[k]);
                  return o;
                }
                return item;
              };
              const rebased = rebase(items);
              if (Array.isArray(rebased)) contentList.push(...rebased); else contentList.push(rebased);
            } catch (err) {
              notes.push(`slice ${s.start}-${s.end}: content list unreadable (${err instanceof Error ? err.message : String(err)})`);
            }
          }
        }
      } finally {
        try { rmSync(tmpBase, { recursive: true, force: true }); } catch { /* ignore */ }
      }

      const mdPath = join(outDir, `${name}.md`);
      const fh = createWriteStream(mdPath);
      for (const part of mdParts) fh.write(part + "\n");
      await new Promise<void>((resolve, reject) => { fh.on("error", reject); fh.end(resolve); });
      if (contentList.length) {
        const cfh = createWriteStream(join(outDir, `${name}_content.json`));
        cfh.write(JSON.stringify(contentList));
        await new Promise<void>((resolve, reject) => { cfh.on("error", reject); cfh.end(resolve); });
      }

      const titleMatch = mdParts[0]?.match(/^#\s+(.+)/m);
      let text = `Merged ${mdParts.length}/${slices.length} slices (pages 1-${slices[slices.length - 1].s.end}) -> ${mdPath}`;
      if (titleMatch) text += `\nTitle: "${titleMatch[1].trim().slice(0, 120)}"`;
      text += `\nImages: ${imageCount} | content list items: ${contentList.length} (page_idx is whole-document, 0-based)`;
      if (notes.length) text += `\n\nNotes:\n${notes.join("\n")}`;
      return { content: [{ type: "text", text }] };
    }
  );

  return server.server;
}

// Sandbox server for Smithery scanning (no real credentials needed)
export function createSandboxServer() {
  return createServer({
    config: {
      mineruApiKey: "sandbox-key",
      mineruBaseUrl: "https://mineru.net/api/v4",
      mineruDefaultModel: "pipeline",
    },
  });
}

// STDIO mode (npx, local dev, Claude Code)
async function main() {
  const config: Config = {
    mineruApiKey: process.env.MINERU_API_KEY || "",
    mineruBaseUrl: process.env.MINERU_BASE_URL || "https://mineru.net/api/v4",
    mineruDefaultModel: (process.env.MINERU_DEFAULT_MODEL as "pipeline" | "vlm") || "pipeline",
  };

  const server = createServer({ config });
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("MinerU MCP server running (stdio mode)");
}

// Only run stdio when executed directly (not when imported by Smithery CLI)
const isDirectRun = process.argv[1] && (
  process.argv[1].endsWith('index.js') ||
  process.argv[1].endsWith('index.ts')
);
if (isDirectRun) {
  main().catch((error) => {
    console.error("Fatal error:", error);
    process.exit(1);
  });
}
