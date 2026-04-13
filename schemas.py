from typing import Any

from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    job_id: str
    filename: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    download_url: str
    message: str


class ProcessRequest(BaseModel):
    job_id: str
    remove_duplicates: bool = False
    sort_column: str | None = None
    sort_order: str = "asc"
    filter_column: str | None = None
    filter_value: str | None = None
    keep_columns: list[str] = Field(default_factory=list)


class ProcessResponse(BaseModel):
    job_id: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    download_url: str
    message: str


class ErrorResponse(BaseModel):
    detail: str
