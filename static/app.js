const state = {
    jobId: null,
    filename: "",
    columns: [],
    sourceColumns: [],
    rowCount: 0,
    downloadUrl: "",
};

const elements = {
    uploadForm: document.getElementById("upload-form"),
    processForm: document.getElementById("process-form"),
    fileInput: document.getElementById("csv-file"),
    uploadButton: document.getElementById("upload-button"),
    applyButton: document.getElementById("apply-button"),
    downloadButton: document.getElementById("download-button"),
    workspace: document.getElementById("workspace"),
    statusMessage: document.getElementById("status-message"),
    errorMessage: document.getElementById("error-message"),
    fileSummary: document.getElementById("file-summary"),
    rowCount: document.getElementById("row-count"),
    previewEmpty: document.getElementById("preview-empty"),
    previewTableWrapper: document.getElementById("preview-table-wrapper"),
    previewHead: document.querySelector("#preview-table thead"),
    previewBody: document.querySelector("#preview-table tbody"),
    removeDuplicates: document.getElementById("remove-duplicates"),
    sortColumn: document.getElementById("sort-column"),
    sortOrder: document.getElementById("sort-order"),
    filterColumn: document.getElementById("filter-column"),
    filterValue: document.getElementById("filter-value"),
    keepColumns: document.getElementById("keep-columns"),
};

elements.uploadForm.addEventListener("submit", handleUpload);
elements.processForm.addEventListener("submit", handleProcess);
elements.downloadButton.addEventListener("click", handleDownload);

function showStatus(message) {
    elements.statusMessage.textContent = message;
    elements.statusMessage.classList.remove("hidden");
}

function hideStatus() {
    elements.statusMessage.textContent = "";
    elements.statusMessage.classList.add("hidden");
}

function showError(message) {
    elements.errorMessage.textContent = message;
    elements.errorMessage.classList.remove("hidden");
}

function hideError() {
    elements.errorMessage.textContent = "";
    elements.errorMessage.classList.add("hidden");
}

function setUploadPending(isPending) {
    elements.uploadButton.disabled = isPending;
    elements.uploadButton.textContent = isPending ? "Uploading..." : "Upload CSV";
}

function setProcessPending(isPending) {
    elements.applyButton.disabled = isPending;
    elements.applyButton.textContent = isPending ? "Applying..." : "Apply changes";
}

async function handleUpload(event) {
    event.preventDefault();
    hideError();
    hideStatus();

    const selectedFile = elements.fileInput.files[0];
    if (!selectedFile) {
        showError("Please choose a CSV file before uploading.");
        return;
    }

    const formData = new FormData();
    formData.append("file", selectedFile);

    setUploadPending(true);

    try {
        const response = await fetch("/api/upload", {
            method: "POST",
            body: formData,
        });
        const payload = await response.json();

        if (!response.ok) {
            throw new Error(payload.detail || "Upload failed.");
        }

        state.jobId = payload.job_id;
        state.filename = payload.filename;
        state.columns = payload.columns;
        state.sourceColumns = payload.columns;
        state.rowCount = payload.row_count;
        state.downloadUrl = payload.download_url;

        renderWorkspace(payload.filename, payload.columns, payload.rows, payload.row_count);
        updateDownloadButton();
        showStatus(payload.message);
    } catch (error) {
        showError(error.message || "Upload failed.");
    } finally {
        setUploadPending(false);
    }
}

async function handleProcess(event) {
    event.preventDefault();
    hideError();
    hideStatus();

    if (!state.jobId) {
        showError("Upload a CSV file before applying changes.");
        return;
    }

    const payload = {
        job_id: state.jobId,
        remove_duplicates: elements.removeDuplicates.checked,
        sort_column: elements.sortColumn.value || null,
        sort_order: elements.sortOrder.value,
        filter_column: elements.filterColumn.value || null,
        filter_value: elements.filterValue.value || null,
        keep_columns: getSelectedKeepColumns(),
    };

    setProcessPending(true);

    try {
        const response = await fetch("/api/process", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(payload),
        });
        const result = await response.json();

        if (!response.ok) {
            throw new Error(result.detail || "Processing failed.");
        }

        state.columns = result.columns;
        state.rowCount = result.row_count;
        state.downloadUrl = result.download_url;

        renderPreview(result.columns, result.rows, result.row_count);
        rebuildColumnControls(state.sourceColumns);
        updateFileSummary();
        updateDownloadButton();
        showStatus(result.message);
    } catch (error) {
        showError(error.message || "Processing failed.");
    } finally {
        setProcessPending(false);
    }
}

function handleDownload(event) {
    if (!state.jobId || !state.downloadUrl) {
        event.preventDefault();
        showError("Upload and process a CSV file before downloading it.");
    }
}

function renderWorkspace(filename, columns, rows, rowCount) {
    elements.workspace.classList.remove("hidden");
    updateFileSummary();
    rebuildColumnControls(columns);
    renderPreview(columns, rows, rowCount);
}

function updateFileSummary() {
    const label = state.filename || "Current CSV";
    elements.fileSummary.textContent = `${label} · ${state.rowCount} rows in current result`;
}

function rebuildColumnControls(columns) {
    const preservedSort = elements.sortColumn.value;
    const preservedFilter = elements.filterColumn.value;
    const preservedKeep = new Set(getSelectedKeepColumns());

    fillSelect(elements.sortColumn, columns, "No sorting", preservedSort);
    fillSelect(elements.filterColumn, columns, "No filter", preservedFilter);

    elements.keepColumns.innerHTML = "";
    if (!columns.length) {
        elements.keepColumns.innerHTML = "<p class=\"helper-text\">No columns available.</p>";
        return;
    }

    columns.forEach((column) => {
        const item = document.createElement("label");
        item.className = "checkbox-item";

        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.value = column;
        checkbox.checked = preservedKeep.has(column);

        const text = document.createElement("span");
        text.textContent = column;

        item.append(checkbox, text);
        elements.keepColumns.appendChild(item);
    });
}

function fillSelect(selectElement, columns, placeholder, preservedValue) {
    selectElement.innerHTML = "";

    const emptyOption = document.createElement("option");
    emptyOption.value = "";
    emptyOption.textContent = placeholder;
    selectElement.appendChild(emptyOption);

    columns.forEach((column) => {
        const option = document.createElement("option");
        option.value = column;
        option.textContent = column;
        selectElement.appendChild(option);
    });

    if (columns.includes(preservedValue)) {
        selectElement.value = preservedValue;
    }
}

function renderPreview(columns, rows, rowCount) {
    elements.rowCount.textContent = `${rowCount} rows`;

    if (!columns.length) {
        elements.previewEmpty.textContent = "No columns found in this CSV file.";
        elements.previewEmpty.classList.remove("hidden");
        elements.previewTableWrapper.classList.add("hidden");
        return;
    }

    elements.previewHead.innerHTML = "";
    elements.previewBody.innerHTML = "";

    const headerRow = document.createElement("tr");
    columns.forEach((column) => {
        const th = document.createElement("th");
        th.textContent = column;
        headerRow.appendChild(th);
    });
    elements.previewHead.appendChild(headerRow);

    if (!rows.length) {
        const emptyRow = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = columns.length;
        td.textContent = "The file has headers but no data rows in the current result.";
        emptyRow.appendChild(td);
        elements.previewBody.appendChild(emptyRow);
    } else {
        rows.forEach((row) => {
            const tr = document.createElement("tr");
            columns.forEach((column) => {
                const td = document.createElement("td");
                const value = row[column];
                td.textContent = value === null || value === undefined ? "" : String(value);
                tr.appendChild(td);
            });
            elements.previewBody.appendChild(tr);
        });
    }

    elements.previewEmpty.classList.add("hidden");
    elements.previewTableWrapper.classList.remove("hidden");
}

function updateDownloadButton() {
    if (state.jobId && state.downloadUrl) {
        elements.downloadButton.href = state.downloadUrl;
        elements.downloadButton.classList.remove("is-disabled");
        return;
    }

    elements.downloadButton.href = "#";
    elements.downloadButton.classList.add("is-disabled");
}

function getSelectedKeepColumns() {
    return Array.from(elements.keepColumns.querySelectorAll("input[type='checkbox']:checked"))
        .map((checkbox) => checkbox.value);
}

updateDownloadButton();
