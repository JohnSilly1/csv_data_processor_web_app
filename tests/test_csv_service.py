import asyncio
from pathlib import Path
import shutil
from types import SimpleNamespace
from uuid import uuid4

import pytest

import csv_service


@pytest.fixture()
def temp_uploads_dir(monkeypatch: pytest.MonkeyPatch) -> Path:
    test_root = Path(__file__).resolve().parent.parent / ".test-work" / uuid4().hex
    monkeypatch.setattr(csv_service, "UPLOADS_DIR", test_root / "uploads")
    csv_service.ensure_uploads_dir()
    yield csv_service.UPLOADS_DIR
    shutil.rmtree(test_root, ignore_errors=True)


def build_payload(**overrides):
    data = {
        "job_id": "job-123",
        "remove_duplicates": False,
        "sort_column": None,
        "sort_order": "asc",
        "filter_column": None,
        "filter_value": None,
        "keep_columns": [],
    }
    data.update(overrides)
    return SimpleNamespace(**data)


class FakeUploadFile:
    def __init__(self, filename: str, content: bytes) -> None:
        self.filename = filename
        self._content = content

    async def read(self) -> bytes:
        return self._content


def create_job(content: bytes, filename: str = "sample.csv") -> str:
    result = asyncio.run(csv_service.save_uploaded_csv(FakeUploadFile(filename, content)))
    return result["job_id"]


def test_read_valid_csv_bytes():
    content = b"name,city\nAlice,London\nBob,Paris\n"
    dataframe, delimiter = csv_service.read_csv_bytes(content)

    assert delimiter == ","
    assert dataframe.columns.tolist() == ["name", "city"]
    assert len(dataframe.index) == 2


def test_empty_file_raises_validation_error():
    with pytest.raises(csv_service.CSVValidationError, match="empty"):
        csv_service.read_csv_bytes(b"")


def test_header_only_csv_is_accepted():
    content = b"name,city\n"
    dataframe, delimiter = csv_service.read_csv_bytes(content)

    assert delimiter == ","
    assert dataframe.columns.tolist() == ["name", "city"]
    assert dataframe.empty


def test_remove_duplicates_drops_full_duplicate_rows():
    dataframe, _ = csv_service.read_csv_bytes(b"name,city\nAlice,London\nAlice,London\nBob,Paris\n")
    payload = build_payload(remove_duplicates=True)

    result = csv_service.apply_operations(dataframe, payload)

    assert result.to_dict(orient="records") == [
        {"name": "Alice", "city": "London"},
        {"name": "Bob", "city": "Paris"},
    ]


def test_filter_rows_uses_case_insensitive_contains():
    dataframe, _ = csv_service.read_csv_bytes(b"name,city\nAlice,London\nBob,Berlin\nCarla,london suburbs\n")
    payload = build_payload(filter_column="city", filter_value="lOnDoN")

    result = csv_service.apply_operations(dataframe, payload)

    assert result.to_dict(orient="records") == [
        {"name": "Alice", "city": "London"},
        {"name": "Carla", "city": "london suburbs"},
    ]


def test_filter_treats_user_input_as_plain_text():
    dataframe, _ = csv_service.read_csv_bytes(b"name,note\nAlice,[test]\nBob,test\n")
    payload = build_payload(filter_column="note", filter_value="[test]")

    result = csv_service.apply_operations(dataframe, payload)

    assert result.to_dict(orient="records") == [{"name": "Alice", "note": "[test]"}]


def test_sort_works_in_both_directions():
    dataframe, _ = csv_service.read_csv_bytes(b"name,score\nAlice,12\nBob,5\nCarla,19\n")

    ascending = csv_service.apply_operations(dataframe, build_payload(sort_column="score", sort_order="asc"))
    descending = csv_service.apply_operations(dataframe, build_payload(sort_column="score", sort_order="desc"))

    assert ascending["score"].tolist() == [5, 12, 19]
    assert descending["score"].tolist() == [19, 12, 5]


def test_keep_selected_columns_only_returns_requested_columns():
    dataframe, _ = csv_service.read_csv_bytes(b"name,city,score\nAlice,London,12\n")
    payload = build_payload(keep_columns=["score", "name"])

    result = csv_service.apply_operations(dataframe, payload)

    assert result.columns.tolist() == ["score", "name"]
    assert result.to_dict(orient="records") == [{"score": 12, "name": "Alice"}]


def test_missing_column_raises_validation_error():
    dataframe, _ = csv_service.read_csv_bytes(b"name,city\nAlice,London\n")

    with pytest.raises(csv_service.CSVValidationError, match="Column not found"):
        csv_service.apply_operations(dataframe, build_payload(sort_column="age"))


def test_semicolon_delimiter_is_detected():
    dataframe, delimiter = csv_service.read_csv_bytes("name;city\nAlice;Berlin\n".encode("utf-8"))

    assert delimiter == ";"
    assert dataframe.to_dict(orient="records") == [{"name": "Alice", "city": "Berlin"}]


def test_process_csv_rewrites_current_file_from_original(temp_uploads_dir: Path):
    job_id = create_job(b"name,city\nAlice,London\nAlice,London\nBob,Paris\n")
    payload = build_payload(
        job_id=job_id,
        remove_duplicates=True,
        keep_columns=["name"],
    )

    result = csv_service.process_csv(payload)
    current_file = temp_uploads_dir / job_id / "current.csv"

    assert result["row_count"] == 2
    assert result["columns"] == ["name"]
    assert current_file.exists()
    assert "name" in current_file.read_text(encoding="utf-8-sig")


def test_download_info_returns_processed_filename(temp_uploads_dir: Path):
    job_id = create_job(b"name,city\nAlice,London\n", filename="sales report.csv")

    file_path, download_name = csv_service.download_info(job_id)

    assert file_path.name == "current.csv"
    assert download_name == "sales_report_processed.csv"
