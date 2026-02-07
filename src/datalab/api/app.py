from __future__ import annotations

import logging
import os
from html import escape
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from datalab.api.job_store import SQLiteJobStore
from datalab.clean import run_pipeline
from datalab.config import load_app_config, load_schema_config
from datalab.jd.analyze import generate_jd_market_report
from datalab.logging_utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging("INFO")


class APIError(BaseModel):
    code: str
    message: str
    request_id: str


class PipelineRunRequest(BaseModel):
    input_path: str = Field(..., description="Input file or directory path.")
    output_dir: str = Field(..., description="Output directory path.")
    topk: int = Field(default=5, ge=1, le=50)
    schema_config_path: str | None = Field(
        default=None, description="Optional schema config YAML path."
    )
    app_config_path: str | None = Field(
        default=None, description="Optional app config YAML path."
    )
    generate_market_report: bool = True


class PipelineRunResponse(BaseModel):
    job_id: str
    status: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    outputs: dict[str, str] = Field(default_factory=dict)
    error_message: str | None = None


def _resolve_default_db_path() -> str:
    return os.getenv("DATALAB_API_JOB_DB", "data/api_jobs.sqlite3")


def _error_response(status_code: int, code: str, message: str, request_id: str) -> JSONResponse:
    payload = APIError(code=code, message=message, request_id=request_id)
    return JSONResponse(status_code=status_code, content=payload.model_dump())


def _markdown_to_html_basic(markdown_text: str) -> str:
    lines = markdown_text.splitlines()
    out: list[str] = []
    i = 0
    in_list = False

    while i < len(lines):
        line = lines[i].rstrip("\n")
        stripped = line.strip()

        if not stripped:
            if in_list:
                out.append("</ul>")
                in_list = False
            i += 1
            continue

        if stripped.startswith("### "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h3>{escape(stripped[4:])}</h3>")
            i += 1
            continue
        if stripped.startswith("## "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h2>{escape(stripped[3:])}</h2>")
            i += 1
            continue
        if stripped.startswith("# "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h1>{escape(stripped[2:])}</h1>")
            i += 1
            continue

        # Markdown table (header + align row + body rows)
        if stripped.startswith("|") and i + 1 < len(lines):
            align = lines[i + 1].strip()
            if align.startswith("|") and set(align.replace("|", "").replace(" ", "")) <= {"-",
                ":"}:
                if in_list:
                    out.append("</ul>")
                    in_list = False
                header_cells = [escape(c.strip()) for c in stripped.strip("|").split("|")]
                out.append("<table><thead><tr>")
                out.extend([f"<th>{c}</th>" for c in header_cells])
                out.append("</tr></thead><tbody>")
                i += 2
                while i < len(lines):
                    row = lines[i].strip()
                    if not row.startswith("|"):
                        break
                    cells = [escape(c.strip()) for c in row.strip("|").split("|")]
                    out.append("<tr>")
                    out.extend([f"<td>{c}</td>" for c in cells])
                    out.append("</tr>")
                    i += 1
                out.append("</tbody></table>")
                continue

        if stripped.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            content = stripped[2:]
            out.append(f"<li>{escape(content)}</li>")
            i += 1
            continue

        if in_list:
            out.append("</ul>")
            in_list = False
        out.append(f"<p>{escape(stripped)}</p>")
        i += 1

    if in_list:
        out.append("</ul>")
    return "".join(out)


def _load_markdown_as_html(path: str | None) -> str:
    if not path:
        return "<p>No report path found.</p>"
    report_path = Path(path)
    if not report_path.exists():
        return f"<p>Report file not found: <code>{escape(str(report_path))}</code></p>"
    text = report_path.read_text(encoding="utf-8", errors="replace")
    return _markdown_to_html_basic(text)


def _build_report_page(job_id: str, status: str, outputs: dict[str, str], error_message: str | None) -> str:
    quality_html = _load_markdown_as_html(outputs.get("quality_report_md"))
    market_html = _load_markdown_as_html(outputs.get("market_report_md"))
    err = f"<p class='error'>{escape(error_message)}</p>" if error_message else ""
    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>DataLab Reports - {escape(job_id)}</title>
    <style>
      body {{ font-family: Segoe UI, sans-serif; max-width: 1100px; margin: 1.2rem auto; padding: 0 1rem; color: #222; }}
      .meta {{ border: 1px solid #ddd; border-radius: 10px; padding: 0.75rem 1rem; background: #fafafa; margin-bottom: 1rem; }}
      .error {{ color: #b42318; font-weight: 600; }}
      .section {{ border: 1px solid #e4e7ec; border-radius: 10px; padding: 1rem; margin-top: 1rem; }}
      table {{ width: 100%; border-collapse: collapse; margin: 0.6rem 0; font-size: 14px; }}
      th, td {{ border: 1px solid #d0d5dd; padding: 0.45rem 0.55rem; text-align: left; vertical-align: top; }}
      thead th {{ background: #f2f4f7; }}
      code {{ background: #f2f4f7; padding: 0.1rem 0.3rem; border-radius: 4px; }}
      h1, h2, h3 {{ margin-top: 0.9rem; margin-bottom: 0.45rem; }}
      ul {{ margin-top: 0.2rem; }}
      .toplinks a {{ margin-right: 0.8rem; }}
    </style>
  </head>
  <body>
    <h1>DataLab Pipeline Report</h1>
    <div class="meta">
      <div><strong>job_id:</strong> <code>{escape(job_id)}</code></div>
      <div><strong>status:</strong> <code>{escape(status)}</code></div>
      {err}
      <div class="toplinks">
        <a href="/">Back Home</a>
        <a href="/pipeline/{escape(job_id)}">JSON Status</a>
      </div>
    </div>
    <div class="section">
      <h2>Data Quality Report</h2>
      {quality_html}
    </div>
    <div class="section">
      <h2>JD Market Report</h2>
      {market_html}
    </div>
  </body>
</html>
    """.strip()


def _build_home_page() -> str:
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>DataLab Web</title>
    <style>
      body { font-family: Segoe UI, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; color: #222; }
      h1 { margin-bottom: 0.3rem; }
      .hint { color: #666; margin-top: 0; }
      .card { border: 1px solid #ddd; border-radius: 10px; padding: 1rem; margin-bottom: 1rem; }
      label { display: block; margin-top: 0.75rem; font-weight: 600; }
      input[type="text"], input[type="number"] { width: 100%; box-sizing: border-box; padding: 0.6rem; border: 1px solid #ccc; border-radius: 8px; }
      button { margin-top: 1rem; padding: 0.6rem 1rem; border: 0; border-radius: 8px; background: #0a66c2; color: white; cursor: pointer; }
      button:disabled { background: #8bb5e2; cursor: not-allowed; }
      pre { background: #f7f7f7; border-radius: 8px; padding: 1rem; overflow: auto; white-space: pre-wrap; }
      .row { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
      @media (max-width: 720px) { .row { grid-template-columns: 1fr; } }
    </style>
  </head>
  <body>
    <h1>DataLab Web UI</h1>
    <p class="hint">Submit a pipeline job and watch status. API docs: <a href="/docs" target="_blank">/docs</a></p>

    <div class="card">
      <label for="inputPath">input_path</label>
      <input id="inputPath" type="text" value="data/sample" />

      <label for="outputDir">output_dir</label>
      <input id="outputDir" type="text" value="data/web_run" />

      <div class="row">
        <div>
          <label for="topk">topk</label>
          <input id="topk" type="number" min="1" max="50" value="5" />
        </div>
        <div>
          <label for="genReport">generate_market_report (true/false)</label>
          <input id="genReport" type="text" value="true" />
        </div>
      </div>

      <button id="runBtn" type="button">Run Pipeline</button>
    </div>

    <div class="card">
      <h3>Result</h3>
      <pre id="result">Ready.</pre>
      <div id="links"></div>
    </div>

    <script>
      const runBtn = document.getElementById("runBtn");
      const result = document.getElementById("result");
      const links = document.getElementById("links");

      function asBool(text) {
        return String(text).trim().toLowerCase() !== "false";
      }

      async function pollJob(jobId) {
        while (true) {
          const res = await fetch(`/pipeline/${jobId}`);
          const data = await res.json();
          result.textContent = JSON.stringify(data, null, 2);
          if (data.status === "succeeded" || data.status === "failed") {
            links.innerHTML = `<p><a href="/pipeline/${jobId}/view" target="_blank">Open Web Report</a></p>`;
            return;
          }
          await new Promise(r => setTimeout(r, 800));
        }
      }

      runBtn.addEventListener("click", async () => {
        runBtn.disabled = true;
        links.innerHTML = "";
        result.textContent = "Submitting job...";
        try {
          const payload = {
            input_path: document.getElementById("inputPath").value.trim(),
            output_dir: document.getElementById("outputDir").value.trim(),
            topk: Number(document.getElementById("topk").value),
            generate_market_report: asBool(document.getElementById("genReport").value)
          };

          const res = await fetch("/pipeline/run", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
          });
          const data = await res.json();
          result.textContent = JSON.stringify(data, null, 2);
          if (!res.ok) {
            return;
          }
          await pollJob(data.job_id);
        } catch (err) {
          result.textContent = String(err);
        } finally {
          runBtn.disabled = false;
        }
      });
    </script>
  </body>
</html>
    """.strip()


def _run_pipeline_job(payload: PipelineRunRequest) -> dict[str, str]:
    output_dir = Path(payload.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    schema = load_schema_config(payload.schema_config_path, payload.app_config_path)
    app_config = load_app_config(payload.app_config_path)
    clean_cfg = app_config.get("clean", {}) if isinstance(app_config.get("clean"), dict) else {}
    skill_dictionary = clean_cfg.get("skill_dictionary")
    run_pipeline(
        input_path=payload.input_path,
        output_path=str(output_dir),
        schema=schema,
        topk=payload.topk,
        skill_dictionary=skill_dictionary if isinstance(skill_dictionary, dict) else None,
    )

    outputs: dict[str, str] = {
        "cleaned_parquet": str(output_dir / "cleaned.parquet"),
        "metrics_json": str(output_dir / "metrics.json"),
        "quality_report_md": str(output_dir / "data_quality_report.md"),
    }
    if payload.generate_market_report:
        market_report_path = output_dir / "jd_market_report.md"
        generate_jd_market_report(output_dir / "cleaned.parquet", market_report_path)
        outputs["market_report_md"] = str(market_report_path)
    return outputs


def _execute_pipeline_job(
    *,
    store: SQLiteJobStore,
    payload: PipelineRunRequest,
    job_id: str,
    request_id: str,
) -> None:
    logger.info("pipeline_job_running request_id=%s job_id=%s", request_id, job_id)
    store.update_job(job_id, status="running")
    try:
        outputs = _run_pipeline_job(payload)
        store.update_job(job_id, status="succeeded", outputs=outputs)
        logger.info("pipeline_job_succeeded request_id=%s job_id=%s", request_id, job_id)
    except Exception as exc:
        store.update_job(job_id, status="failed", outputs={}, error_message=str(exc))
        logger.exception("pipeline_job_failed request_id=%s job_id=%s", request_id, job_id)


def create_app(job_db_path: str | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.job_store = SQLiteJobStore(job_db_path or _resolve_default_db_path())
        app.state.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="datalab-api")
        yield
        app.state.executor.shutdown(wait=False, cancel_futures=False)

    app = FastAPI(title="DataLab API", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid4()))
        request.state.request_id = request_id
        logger.info("request_start request_id=%s path=%s", request_id, request.url.path)
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request_end request_id=%s path=%s status=%s",
            request_id,
            request.url.path,
            response.status_code,
        )
        return response

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        request_id = getattr(request.state, "request_id", "unknown")
        detail = exc.detail if isinstance(exc.detail, str) else "HTTP error"
        return _error_response(exc.status_code, "http_error", detail, request_id)

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
        request_id = getattr(request.state, "request_id", "unknown")
        return _error_response(422, "validation_error", str(exc), request_id)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        request_id = getattr(request.state, "request_id", "unknown")
        logger.exception("Unhandled error request_id=%s", request_id)
        return _error_response(500, "internal_error", str(exc), request_id)

    @app.get("/", response_class=HTMLResponse)
    def home():
        return _build_home_page()

    @app.post("/pipeline/run", response_model=PipelineRunResponse)
    def run_pipeline_endpoint(payload: PipelineRunRequest, request: Request):
        request_id = getattr(request.state, "request_id", "unknown")
        job_id = str(uuid4())
        store: SQLiteJobStore = request.app.state.job_store
        executor: ThreadPoolExecutor = request.app.state.executor

        store.create_job(job_id, status="queued")
        logger.info("pipeline_job_queued request_id=%s job_id=%s", request_id, job_id)

        executor.submit(
            _execute_pipeline_job,
            store=store,
            payload=payload,
            job_id=job_id,
            request_id=request_id,
        )
        return PipelineRunResponse(job_id=job_id, status="queued")

    @app.get("/pipeline/{job_id}", response_model=JobStatusResponse)
    def get_pipeline_status(job_id: str, request: Request):
        request_id = getattr(request.state, "request_id", "unknown")
        store: SQLiteJobStore = request.app.state.job_store
        record = store.get_job(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"job_id not found: {job_id}")
        logger.info(
            "pipeline_status request_id=%s job_id=%s status=%s",
            request_id,
            job_id,
            record["status"],
        )
        return JobStatusResponse(**record)

    @app.get("/pipeline/{job_id}/view", response_class=HTMLResponse)
    def view_pipeline_report(job_id: str, request: Request):
        store: SQLiteJobStore = request.app.state.job_store
        record = store.get_job(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"job_id not found: {job_id}")
        return _build_report_page(
            job_id=job_id,
            status=record["status"],
            outputs=record.get("outputs", {}),
            error_message=record.get("error_message"),
        )

    return app


app = create_app()
