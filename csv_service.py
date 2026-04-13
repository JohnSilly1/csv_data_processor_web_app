import csv
import io
import json
import re
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
from fastapi import UploadFile
from pandas.errors import EmptyDataError, ParserError


BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
PREVIEW_LIMIT = 20
SUPPORTED_ENCODINGS = ("utf-8-sig", "utf-8", "cp1251")
SUPPORTED_DELIMITERS = [",", ";", "\t", "|"]


class CSVProcessingError(Exception):
    pass


class CSVValidationError(CSVProcessingError):
    pass


class CSVJobNotFoundError(CSVProcessingError):
    pass


def ensure_uploads_dir() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)


async def save_uploaded_csv(file: UploadFile) -> dict[str, Any]:
    ensure_uploads_dir()

    filename = file.filename or ""
    if not filename.strip():
        raise CSVValidationError("Please choose a CSV file before uploading.")

    if not filename.lower().endswith(".csv"):
        raise CSVValidationError("Only CSV files are supported.")

    content = await file.read()
    if not content or not content.strip():
        raise CSVValidationError("The uploaded CSV file is empty.")

    dataframe, delimiter = read_csv_bytes(content)

    job_id = uuid4().hex
    job_dir = UPLOADS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    original_path = job_dir / "original.csv"
    current_path = job_dir / "current.csv"
    meta_path = job_dir / "meta.json"

    original_path.write_bytes(content)
    write_dataframe(dataframe, current_path, delimiter)

    meta = {
        "original_filename": Path(filename).name,
        "delimiter": delimiter,
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return {
        "job_id": job_id,
        "filename": meta["original_filename"],
        "columns": dataframe.columns.tolist(),
        "rows": dataframe_to_rows(dataframe),
        "row_count": int(len(dataframe.index)),
        "download_url": f"/api/download?job_id={job_id}",
        "message": "CSV uploaded successfully.",
    }


def process_csv(payload: Any) -> dict[str, Any]:
    ensure_uploads_dir()
    job_dir = get_job_dir(payload.job_id)
    original_path = job_dir / "original.csv"
    meta = read_meta(job_dir)

    dataframe, _ = read_csv_bytes(
        original_path.read_bytes(),
        preferred_delimiter=meta["delimiter"],
    )
    dataframe = apply_operations(dataframe, payload)

    current_path = job_dir / "current.csv"
    write_dataframe(dataframe, current_path, meta["delimiter"])

    return {
        "job_id": payload.job_id,
        "columns": dataframe.columns.tolist(),
        "rows": dataframe_to_rows(dataframe),
        "row_count": int(len(dataframe.index)),
        "download_url": f"/api/download?job_id={payload.job_id}",
        "message": "CSV processed successfully.",
    }


def download_info(job_id: str) -> tuple[Path, str]:
    if not job_id.strip():
        raise CSVValidationError("Upload a CSV file before downloading a result.")

    job_dir = get_job_dir(job_id)
    current_path = job_dir / "current.csv"
    if not current_path.exists():
        raise CSVValidationError("No processed CSV is available for download yet.")

    meta = read_meta(job_dir)
    original_stem = Path(meta["original_filename"]).stem
    safe_name = slugify_filename(original_stem) or "processed_data"
    return current_path, f"{safe_name}_processed.csv"


def get_job_dir(job_id: str) -> Path:
    if not job_id or not job_id.strip():
        raise CSVValidationError("A valid job ID is required.")

    job_dir = UPLOADS_DIR / job_id
    if not job_dir.exists():
        raise CSVJobNotFoundError("The uploaded CSV session was not found.")

    return job_dir


def read_meta(job_dir: Path) -> dict[str, str]:
    meta_path = job_dir / "meta.json"
    if not meta_path.exists():
        raise CSVJobNotFoundError("The uploaded CSV session metadata was not found.")

    return json.loads(meta_path.read_text(encoding="utf-8"))


def read_csv_bytes(
    content: bytes,
    preferred_delimiter: str | None = None,
) -> tuple[pd.DataFrame, str]:
    if not content or not content.strip():
        raise CSVValidationError("The uploaded CSV file is empty.")

    for encoding in SUPPORTED_ENCODINGS:
        try:
            text = content.decode(encoding)
            delimiter = preferred_delimiter or detect_delimiter(text)
            dataframe = pd.read_csv(
                io.StringIO(text),
                sep=delimiter,
                engine="python",
            )
            return dataframe, delimiter
        except UnicodeDecodeError:
            continue
        except EmptyDataError as exc:
            raise CSVValidationError("The uploaded CSV file is empty.") from exc
        except ParserError:
            continue

    raise CSVValidationError("The file could not be parsed as a CSV document.")


def detect_delimiter(text: str) -> str:
    try:
        sample = "\n".join(text.splitlines()[:10])
        dialect = csv.Sniffer().sniff(sample, delimiters=SUPPORTED_DELIMITERS)
        return dialect.delimiter
    except csv.Error:
        return ","


def apply_operations(dataframe: pd.DataFrame, payload: Any) -> pd.DataFrame:
    result = dataframe.copy()

    if getattr(payload, "remove_duplicates", False):
        result = result.drop_duplicates()

    filter_column = getattr(payload, "filter_column", None)
    filter_value = getattr(payload, "filter_value", None)
    if filter_column:
        validate_columns_exist(result, [filter_column])
        if filter_value and filter_value.strip():
            series = result[filter_column].fillna("").astype(str)
            mask = series.str.contains(filter_value, case=False, na=False, regex=False)
            result = result.loc[mask]

    sort_column = getattr(payload, "sort_column", None)
    if sort_column:
        validate_columns_exist(result, [sort_column])
        sort_order = getattr(payload, "sort_order", "asc")
        if sort_order not in {"asc", "desc"}:
            raise CSVValidationError("Sort order must be either 'asc' or 'desc'.")

        result = result.sort_values(
            by=sort_column,
            ascending=sort_order == "asc",
            kind="mergesort",
        )

    keep_columns = list(getattr(payload, "keep_columns", []) or [])
    if keep_columns:
        validate_columns_exist(result, keep_columns)
        result = result.loc[:, keep_columns]

    return result.reset_index(drop=True)


def validate_columns_exist(dataframe: pd.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in dataframe.columns]
    if missing:
        missing_columns = ", ".join(missing)
        raise CSVValidationError(f"Column not found: {missing_columns}")


def dataframe_to_rows(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    preview = dataframe.head(PREVIEW_LIMIT).copy()
    preview = preview.where(pd.notnull(preview), None)
    return preview.to_dict(orient="records")


def write_dataframe(dataframe: pd.DataFrame, path: Path, delimiter: str) -> None:
    dataframe.to_csv(path, index=False, sep=delimiter, encoding="utf-8-sig")


def slugify_filename(filename: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", filename).strip("._")
