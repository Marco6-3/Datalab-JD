from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from datalab.api.job_store import SQLiteJobStore
from datalab.clean import run_pipeline
from datalab.config import load_schema_config
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


def _run_pipeline_job(payload: PipelineRunRequest) -> dict[str, str]:
    output_dir = Path(payload.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    schema = load_schema_config(payload.schema_config_path, payload.app_config_path)
    run_pipeline(
        input_path=payload.input_path,
        output_path=str(output_dir),
        schema=schema,
        topk=payload.topk,
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

    return app


app = create_app()
