import logging
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from csv_service import (
    CSVJobNotFoundError,
    CSVProcessingError,
    CSVValidationError,
    download_info,
    process_csv,
    save_uploaded_csv,
)
from schemas import ErrorResponse, ProcessRequest, ProcessResponse, UploadResponse


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="CSV Data Processor Web App")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.get("/", response_class=HTMLResponse)
async def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {"request": request},
    )


@app.post(
    "/api/upload",
    response_model=UploadResponse,
    responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
async def upload_csv(file: UploadFile = File(...)) -> UploadResponse:
    try:
        result = await save_uploaded_csv(file)
        return UploadResponse(**result)
    except CSVValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except CSVProcessingError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error during CSV upload")
        raise HTTPException(
            status_code=500,
            detail="Unexpected server error while uploading the file.",
        ) from exc


@app.post(
    "/api/process",
    response_model=ProcessResponse,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
async def process_uploaded_csv(payload: ProcessRequest) -> ProcessResponse:
    try:
        result = process_csv(payload)
        return ProcessResponse(**result)
    except CSVValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except CSVJobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except CSVProcessingError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error during CSV processing")
        raise HTTPException(
            status_code=500,
            detail="Unexpected server error while processing the file.",
        ) from exc


@app.get(
    "/api/download",
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
async def download_csv(job_id: str) -> FileResponse:
    try:
        file_path, download_name = download_info(job_id)
        return FileResponse(
            path=file_path,
            media_type="text/csv",
            filename=download_name,
        )
    except CSVValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except CSVJobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error during CSV download")
        raise HTTPException(
            status_code=500,
            detail="Unexpected server error while downloading the file.",
        ) from exc
