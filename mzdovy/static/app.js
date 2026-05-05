const body = document.body;
const pageId = body.dataset.page || "";
const pageStep = body.dataset.step || "";
const BASE_URL = (body.dataset.baseUrl || "").replace(/\/$/, "");
const api = (path) => BASE_URL + path;

const state = {
  currentImportId: body.dataset.importId ? Number(body.dataset.importId) : null,
  imports: [],
  employees: [],
  employeeFilters: {
    query: "",
    project: "",
    coordinator: "",
    company: "",
  },
  metadata: {
    projects: [],
    coordinators: [],
    companies: [],
  },
  coverage: [],
  incompleteGroups: [],
  exportReady: true,
};

const selectedFiles = [];

const employeeDialog = document.getElementById("employeeDialog");
const employeeForm = document.getElementById("employeeForm");
const employeeDialogTitle = document.getElementById("employeeDialogTitle");
const employeeDialogCopy = document.getElementById("employeeDialogCopy");
const employeeDialogSubmit = document.getElementById("employeeDialogSubmit");
const cancelDialog = document.getElementById("cancelDialog");
const cancelDialogSecondary = document.getElementById("cancelDialogSecondary");
const employeeProjectSelect = document.getElementById("employeeProjectSelect");
const employeeCoordinatorSelect = document.getElementById("employeeCoordinatorSelect");
const employeeOdvodyStrhavame = document.getElementById("employeeOdvodyStrhavame");
const companyOptions = document.getElementById("companyOptions");

const DIRTY_KEY = "payrollEmployeeDataDirty";
const UPLOAD_NOTICE_KEY = "payrollUploadNotice";

const isEmployeeDataDirty = () => window.sessionStorage.getItem(DIRTY_KEY) === "1";
const markEmployeeDataDirty = () => window.sessionStorage.setItem(DIRTY_KEY, "1");
const clearEmployeeDataDirty = () => window.sessionStorage.removeItem(DIRTY_KEY);
const saveUploadNotice = (payload) => window.sessionStorage.setItem(UPLOAD_NOTICE_KEY, JSON.stringify(payload));
const loadUploadNotice = () => {
  const raw = window.sessionStorage.getItem(UPLOAD_NOTICE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
};
const clearUploadNotice = () => window.sessionStorage.removeItem(UPLOAD_NOTICE_KEY);

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  let data = {};
  try {
    data = await response.json();
  } catch (e) {
    data = {};
  }
  if (!response.ok) {
    const error = new Error(data.error || `Požadavek selhal (${response.status})`);
    error.details = data.details || [];
    throw error;
  }
  return data;
}

function transitionNavigate(url) {
  if (!url || url === "#") {
    return;
  }
  if (document.startViewTransition) {
    document.startViewTransition(() => {
      window.location.href = url;
    });
    return;
  }
  window.location.href = url;
}

function installNavigationTransitions() {
  document.querySelectorAll("[data-nav]").forEach((link) => {
    if (link.dataset.navBound === "1") return;
    link.dataset.navBound = "1";
    link.addEventListener("click", (event) => {
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
        return;
      }
      event.preventDefault();
      transitionNavigate(link.getAttribute("href"));
    });
  });
}

function escapeHtml(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatMoney(value) {
  return Number(value || 0).toLocaleString("cs-CZ", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}

function formatDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("cs-CZ");
}

function formatFileSize(bytes) {
  if (!bytes) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

const fileKey = (file) => `${file.name}__${file.size}__${file.lastModified}`;

function setSummary(summary) {
  const previewCount = document.getElementById("previewCount");
  const matchedCount = document.getElementById("matchedCount");
  const missingCount = document.getElementById("missingCount");
  const summaryPeriod = document.getElementById("summaryPeriod");
  if (previewCount) previewCount.textContent = summary ? summary.preview_rows : 0;
  if (matchedCount) matchedCount.textContent = summary ? summary.matched_rows : 0;
  if (missingCount) missingCount.textContent = summary ? summary.missing_rows : 0;
  if (summaryPeriod && summary) summaryPeriod.textContent = summary.period || "Nevyplněno";
}

const formatStatus = (value) => (value === "matched" ? "Spárováno" : "Chybí v databázi");

function renderWarnings(warnings, { hideWhenEmpty = false } = {}) {
  if (!warnings || !warnings.length) {
    return hideWhenEmpty ? '<span class="muted">&mdash;</span>' : "Bez upozornění";
  }
  return `<ul class="warning-list">${warnings.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`;
}

function renderCoverageWarnings(groups) {
  if (!groups || !groups.length) return "";
  const labels = {
    prehled_mezd: "Přehled mezd",
    socialka: "Sociálka",
    zdravotka: "Zdravotka",
  };
  return groups.map((item) => {
    const companyName = item.company_name || "Neznámá firma";
    const periodName = item.period || "Neznámé období";
    const missing = (item.missing_types || []).map((code) => labels[code] || code).join(", ");
    return `• <strong>${escapeHtml(companyName)}</strong> (${escapeHtml(periodName)}): chybí ${escapeHtml(missing)}`;
  }).join("<br>");
}

function renderControlSumCell(row) {
  const parsed = Number(row.control_sum_parsed || 0);
  const expected = Number(row.control_sum_expected || 0);
  if (!parsed && !expected) return "&mdash;";
  const delta = parsed - expected;
  const mismatch = expected > 0 && Math.abs(delta) > 1;
  const negative = parsed < 0;
  const classes = ["control-sum"];
  if (mismatch) classes.push("mismatch");
  if (negative && !mismatch) classes.push("negative");
  return `
    <div class="${classes.join(" ")}">
      <div>${formatMoney(parsed)}</div>
      ${mismatch ? `<div class="cell-hint warn">Očekáváno ${formatMoney(expected)} · rozdíl ${formatMoney(delta)}</div>` : ""}
      ${negative && !mismatch ? `<div class="cell-hint">Záporný zůstatek</div>` : ""}
    </div>
  `;
}

const safeRowPayload = (row) => ({
  id: row.id,
  full_name: row.display_name,
  company_name: row.company_name || "",
});

function renderPreviewRows(rows, { target, allowCreate = false } = {}) {
  if (!target) return;
  if (!rows.length) {
    target.innerHTML = '<tr><td colspan="10" class="empty">Po nahrání souborů se tady objeví společný přehled zaměstnanců, firem a odvodů.</td></tr>';
    return;
  }

  target.innerHTML = rows.map((row) => {
    const mismatch = (row.warnings || []).some((w) => w.includes("Rozd"));
    return `
    <tr class="${row.match_status === "missing" ? "row-missing" : ""} ${mismatch ? "row-mismatch" : ""}">
      <td>${escapeHtml(row.display_name)}</td>
      <td>${escapeHtml(row.company_name) || "&mdash;"}</td>
      <td>${escapeHtml(row.project_name) || "&mdash;"}</td>
      <td>${escapeHtml(row.coordinator_name) || "&mdash;"}</td>
      <td class="num">${formatMoney(row.odvody_platime)}</td>
      <td class="num">${formatMoney(row.odvody_strhavame)}</td>
      <td class="num">${Number(row.mesicni_mzda) > 0 ? formatMoney(row.mesicni_mzda) : '<span class="muted">&mdash;</span>'}</td>
      <td><span class="status ${row.match_status}">${formatStatus(row.match_status)}</span></td>
      <td>${renderWarnings(row.warnings, { hideWhenEmpty: row.match_status === "missing" })}</td>
      <td>${allowCreate && row.match_status === "missing"
          ? `<button class="button secondary small create-employee" data-row='${escapeHtml(JSON.stringify(safeRowPayload(row)))}'>Založit</button>`
          : ""}</td>
    </tr>
  `;
  }).join("");

  if (allowCreate) {
    target.querySelectorAll(".create-employee").forEach((button) => {
      button.addEventListener("click", () => {
        openEmployeeDialog({ mode: "preview-create", row: JSON.parse(button.dataset.row) });
      });
    });
  }
}

function renderHistoryRows(rows) {
  const target = document.getElementById("historyPreviewBody");
  if (!target) return;
  if (!rows.length) {
    target.innerHTML = '<tr><td colspan="8" class="empty">Vybraný import je prázdný.</td></tr>';
    return;
  }
  target.innerHTML = rows.map((row) => `
    <tr>
      <td>${escapeHtml(row.display_name)}</td>
      <td>${escapeHtml(row.company_name) || "&mdash;"}</td>
      <td>${escapeHtml(row.project_name) || "&mdash;"}</td>
      <td>${escapeHtml(row.coordinator_name) || "&mdash;"}</td>
      <td class="num">${formatMoney(row.odvody_platime)}</td>
      <td class="num">${formatMoney(row.odvody_strhavame)}</td>
      <td><span class="status ${row.match_status}">${formatStatus(row.match_status)}</span></td>
      <td>${renderWarnings(row.warnings)}</td>
    </tr>
  `).join("");
}

function renderImportsList(container, items, { activeImportId = null, onSelect, emptyMessage } = {}) {
  if (!container) return;
  if (!items.length) {
    container.innerHTML = `<div class="empty">${emptyMessage || "Zatím tu není žádný import."}</div>`;
    return;
  }
  container.innerHTML = items.map((item) => `
    <button type="button" class="import-item ${activeImportId === item.id ? "active" : ""}" data-import-id="${item.id}">
      <div class="import-topline">
        <strong>${escapeHtml(item.period || "Bez období")}</strong>
        <span>#${item.id}</span>
      </div>
      <div class="import-meta">Soubory: ${item.file_count} · Řádků v přehledu: ${item.preview_rows}</div>
      <div class="import-meta">${formatDateTime(item.created_at)}</div>
    </button>
  `).join("");
  container.querySelectorAll(".import-item").forEach((node) => {
    node.addEventListener("click", () => onSelect(Number(node.dataset.importId)));
  });
}

async function loadImports() {
  const data = await fetchJson(api("/api/imports"));
  state.imports = data.imports || [];
  return state.imports;
}

async function loadPreview(importId) {
  state.currentImportId = importId;
  const data = await fetchJson(api(`/api/imports/${importId}/preview`));
  state.coverage = data.coverage || [];
  state.incompleteGroups = data.incomplete_groups || [];
  state.exportReady = data.export_ready !== false;
  return data;
}

async function loadMetadata() {
  const data = await fetchJson(api("/api/meta"));
  state.metadata = {
    projects: data.projects || [],
    coordinators: data.coordinators || [],
    companies: data.companies || [],
  };
  fillEmployeeMetadataControls();
  fillEmployeeFilters();
}

function rebuildSelectOptions(select, values, emptyLabel) {
  if (!select) return;
  const currentValue = select.value || "";
  const options = [`<option value="">${emptyLabel}</option>`];
  values.forEach((value) => {
    options.push(`<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`);
  });
  if (currentValue && !values.includes(currentValue)) {
    options.push(`<option value="${escapeHtml(currentValue)}">${escapeHtml(currentValue)}</option>`);
  }
  select.innerHTML = options.join("");
  select.value = currentValue;
}

function fillEmployeeMetadataControls() {
  rebuildSelectOptions(employeeProjectSelect, state.metadata.projects, "Bez projektu");
  rebuildSelectOptions(employeeCoordinatorSelect, state.metadata.coordinators, "Bez koordinátora");
  if (companyOptions) {
    companyOptions.innerHTML = state.metadata.companies
      .map((value) => `<option value="${escapeHtml(value)}"></option>`)
      .join("");
  }
}

function fillEmployeeFilters() {
  rebuildSelectOptions(document.getElementById("employeeFilterProject"), state.metadata.projects, "Všechny projekty");
  rebuildSelectOptions(document.getElementById("employeeFilterCoordinator"), state.metadata.coordinators, "Všichni koordinátoři");
  rebuildSelectOptions(document.getElementById("employeeFilterCompany"), state.metadata.companies, "Všechny firmy");
}

function showNotice(node, message, type = "info") {
  if (!node) return;
  node.className = `notice ${type}`;
  node.innerHTML = message;
}

function hideNotice(node) {
  if (!node) return;
  node.className = "notice hidden";
  node.innerHTML = "";
}

function openEmployeeDialog({ mode = "preview-create", row = null, employee = null } = {}) {
  if (!employeeDialog || !employeeForm) return;

  fillEmployeeMetadataControls();
  employeeForm.dataset.mode = mode;
  employeeForm.preview_row_id.value = row ? row.id : "";
  employeeForm.employee_id.value = employee ? employee.id : "";

  const defaults = () => {
    employeeForm.full_name.value = "";
    employeeForm.project_name.value = "";
    employeeForm.coordinator_name.value = "";
    employeeForm.company_name.value = "";
    employeeForm.odvody_strhavame.value = "";
    employeeForm.mesicni_mzda.value = "";
  };

  if (mode === "preview-create") {
    employeeDialogTitle.textContent = "Založit nového zaměstnance";
    employeeDialogCopy.textContent = "Vyplňte projekt, koordinátora, firmu a hodnoty odvodů. Po uložení spusťte přepočet přehledu.";
    employeeDialogSubmit.textContent = "Uložit zaměstnance";
    defaults();
    employeeForm.full_name.value = row ? row.full_name : "";
    employeeForm.company_name.value = row ? row.company_name : "";
    const platime = row && Number(row.odvody_platime) > 0 ? Number(row.odvody_platime) : "";
    employeeForm.odvody_strhavame.value = platime ? platime.toFixed(2).replace(/\.00$/, "") : "";
  } else if (mode === "manual-create") {
    employeeDialogTitle.textContent = "Přidat zaměstnance";
    employeeDialogCopy.textContent = "Nový zaměstnanec se uloží do databáze a použije se při dalším přepočtu.";
    employeeDialogSubmit.textContent = "Přidat zaměstnance";
    defaults();
  } else if (mode === "manual-edit" && employee) {
    employeeDialogTitle.textContent = "Upravit zaměstnance";
    employeeDialogCopy.textContent = "Změna se projeví po nejbližším přepočtu přehledu.";
    employeeDialogSubmit.textContent = "Uložit změny";
    employeeForm.full_name.value = employee.full_name || "";
    employeeForm.project_name.value = employee.project_name || "";
    employeeForm.coordinator_name.value = employee.coordinator_name || "";
    employeeForm.company_name.value = employee.company_name || "";
    employeeForm.odvody_strhavame.value = employee.odvody_strhavame || "";
    employeeForm.mesicni_mzda.value = employee.mesicni_mzda || "";
  }

  fillEmployeeMetadataControls();
  employeeForm.project_name.value = employeeForm.project_name.value || "";
  employeeForm.coordinator_name.value = employeeForm.coordinator_name.value || "";
  employeeDialog.showModal();
}

function closeEmployeeDialog() {
  if (employeeDialog && employeeDialog.open) employeeDialog.close();
}

function filteredEmployees() {
  const { query, project, coordinator, company } = state.employeeFilters;
  const q = (query || "").trim().toLowerCase();
  return state.employees.filter((employee) => {
    if (project && (employee.project_name || "") !== project) return false;
    if (coordinator && (employee.coordinator_name || "") !== coordinator) return false;
    if (company && (employee.company_name || "") !== company) return false;
    if (!q) return true;
    const haystack = [
      employee.full_name,
      employee.project_name,
      employee.coordinator_name,
      employee.company_name,
      employee.notes,
    ].filter(Boolean).join(" ").toLowerCase();
    return haystack.includes(q);
  });
}

function renderEmployees() {
  const employeesBody = document.getElementById("employeesBody");
  const employeesCount = document.getElementById("employeesCount");
  if (!employeesBody || !employeesCount) return;

  const filtered = filteredEmployees();
  employeesCount.textContent = `${filtered.length} z ${state.employees.length} zaměstnanců`;

  if (!filtered.length) {
    const message = state.employees.length
      ? "Nic nevyhovuje zadanému filtru."
      : "Databáze je prázdná. Nahrajte XLSX nebo přidejte prvního zaměstnance ručně.";
    employeesBody.innerHTML = `<tr><td colspan="7" class="empty">${message}</td></tr>`;
    return;
  }

  employeesBody.innerHTML = filtered.map((employee) => `
    <tr>
      <td>
        <div class="employee-name">${escapeHtml(employee.full_name)}</div>
        ${employee.notes ? `<div class="employee-notes">${escapeHtml(employee.notes)}</div>` : ""}
      </td>
      <td>${escapeHtml(employee.project_name) || "&mdash;"}</td>
      <td>${escapeHtml(employee.coordinator_name) || "&mdash;"}</td>
      <td>${escapeHtml(employee.company_name) || "&mdash;"}</td>
      <td class="num">${formatMoney(employee.odvody_strhavame)}</td>
      <td class="num">${formatMoney(employee.mesicni_mzda)}</td>
      <td>
        <div class="row-actions">
          <button class="button secondary small edit-employee" data-employee-id="${employee.id}">Upravit</button>
          <button class="button ghost-danger small delete-employee" data-employee-id="${employee.id}">Smazat</button>
        </div>
      </td>
    </tr>
  `).join("");

  employeesBody.querySelectorAll(".edit-employee").forEach((button) => {
    button.addEventListener("click", () => {
      const employee = state.employees.find((item) => item.id === Number(button.dataset.employeeId));
      if (employee) openEmployeeDialog({ mode: "manual-edit", employee });
    });
  });

  employeesBody.querySelectorAll(".delete-employee").forEach((button) => {
    button.addEventListener("click", async () => {
      const employee = state.employees.find((item) => item.id === Number(button.dataset.employeeId));
      if (!employee) return;
      if (!confirm(`Opravdu smazat zaměstnance "${employee.full_name}"?`)) return;
      try {
        const data = await fetchJson(api(`/api/employees/${employee.id}`), { method: "DELETE" });
        await loadMetadata();
        state.employees = data.employees || [];
        markEmployeeDataDirty();
        renderEmployees();
      } catch (error) {
        alert(`Nepodařilo se smazat zaměstnance: ${error.message}`);
      }
    });
  });
}

async function loadEmployees() {
  const data = await fetchJson(api("/api/employees"));
  state.employees = data.employees || [];
  renderEmployees();
}

function updateReviewNotice() {
  const reviewNotice = document.getElementById("reviewNotice");
  if (!reviewNotice) return;
  const messages = [];
  let noticeType = "info";
  const uploadNotice = loadUploadNotice();

  if (uploadNotice?.skipped_files?.length) {
    noticeType = "warning";
    messages.push(
      `Některé soubory se nepodařilo načíst. Zpracováno: <strong>${uploadNotice.processed_files?.length || 0}</strong>, přeskočeno: <strong>${uploadNotice.skipped_files.length}</strong>.<br>${uploadNotice.skipped_files.map((item) => `• <strong>${escapeHtml(item.filename || "soubor")}</strong>: ${escapeHtml(item.error || "chyba při zpracování")}`).join("<br>")}`
    );
  }

  if (isEmployeeDataDirty()) {
    noticeType = "warning";
    messages.push(
      `Databáze zaměstnanců byla upravena. Než přejdete na export, spusťte prosím <a href="${BASE_URL}/wizard/${state.currentImportId}/recompute" data-nav>přepočet přehledu</a>.`
    );
  }

  if (state.incompleteGroups.length) {
    noticeType = "warning";
    messages.push(
      `Export je zatím blokovaný, protože pro některé firmy chybí kompletní sada reportů.<br>${renderCoverageWarnings(state.incompleteGroups)}`
    );
  }

  if (messages.length) {
    showNotice(reviewNotice, messages.join("<br><br>"), noticeType);
    installNavigationTransitions();
    clearUploadNotice();
    return;
  }
  hideNotice(reviewNotice);
  clearUploadNotice();
}

async function handleEmployeeFormSubmit(event) {
  event.preventDefault();
  const payload = {
    full_name: employeeForm.full_name.value,
    project_name: employeeForm.project_name.value,
    coordinator_name: employeeForm.coordinator_name.value,
    company_name: employeeForm.company_name.value,
    odvody_strhavame: employeeForm.odvody_strhavame.value || 0,
    mesicni_mzda: employeeForm.mesicni_mzda.value || 0,
  };
  const mode = employeeForm.dataset.mode || "preview-create";

  try {
    if (mode === "preview-create") {
      const previewRowId = Number(employeeForm.preview_row_id.value);
      const data = await fetchJson(api(`/api/preview/${previewRowId}/employees`), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await loadMetadata();
      markEmployeeDataDirty();
      closeEmployeeDialog();
      if (pageId === "wizard" && pageStep === "review") {
        setSummary(data.summary);
        renderPreviewRows(data.preview_rows || [], {
          target: document.getElementById("previewBody"),
          allowCreate: true,
        });
        updateReviewNotice();
      }
      if (pageId === "employees") await loadEmployees();
    } else if (mode === "manual-create") {
      const data = await fetchJson(api("/api/employees"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await loadMetadata();
      state.employees = data.employees || [];
      markEmployeeDataDirty();
      renderEmployees();
      closeEmployeeDialog();
    } else if (mode === "manual-edit") {
      const employeeId = Number(employeeForm.employee_id.value);
      const data = await fetchJson(api(`/api/employees/${employeeId}`), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      await loadMetadata();
      state.employees = data.employees || [];
      markEmployeeDataDirty();
      renderEmployees();
      closeEmployeeDialog();
    }
  } catch (error) {
    alert(`Nepodařilo se uložit zaměstnance: ${error.message}`);
  }
}

function bindEmployeeDialog() {
  if (!employeeForm) return;
  employeeForm.addEventListener("submit", handleEmployeeFormSubmit);
  [cancelDialog, cancelDialogSecondary].forEach((button) => {
    if (button) button.addEventListener("click", closeEmployeeDialog);
  });
}

function renderSelectedFiles() {
  const selectedFilesNode = document.getElementById("selectedFiles");
  if (!selectedFilesNode) return;
  if (!selectedFiles.length) {
    selectedFilesNode.className = "selected-files empty-files";
    selectedFilesNode.innerHTML = '<div class="selected-files-empty">Zatím nejsou vybrané žádné soubory.</div>';
    return;
  }
  selectedFilesNode.className = "selected-files";
  selectedFilesNode.innerHTML = selectedFiles.map((file, index) => `
    <div class="selected-file">
      <div>
        <div class="selected-file-name">${escapeHtml(file.name)}</div>
        <div class="selected-file-meta">${formatFileSize(file.size)}</div>
      </div>
      <button type="button" class="selected-file-remove" data-file-index="${index}">Odstranit</button>
    </div>
  `).join("");
  selectedFilesNode.querySelectorAll(".selected-file-remove").forEach((button) => {
    button.addEventListener("click", () => {
      selectedFiles.splice(Number(button.dataset.fileIndex), 1);
      renderSelectedFiles();
    });
  });
}

async function initWizardUpload() {
  const importsList = document.getElementById("importsList");
  const uploadForm = document.getElementById("uploadForm");
  const fileInput = document.getElementById("fileInput");
  const chooseFilesButton = document.getElementById("chooseFilesButton");

  renderSelectedFiles();
  chooseFilesButton?.addEventListener("click", () => fileInput?.click());
  fileInput?.addEventListener("change", () => {
    const existing = new Set(selectedFiles.map((file) => fileKey(file)));
    Array.from(fileInput.files || []).forEach((file) => {
      const key = fileKey(file);
      if (!existing.has(key)) {
        selectedFiles.push(file);
        existing.add(key);
      }
    });
    fileInput.value = "";
    renderSelectedFiles();
  });

  uploadForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!selectedFiles.length) {
      alert("Nejprve vyberte alespoň jeden HTML soubor.");
      return;
    }

    const submitButton = uploadForm.querySelector("button[type=submit]");
    const originalLabel = submitButton?.textContent;
    if (submitButton) {
      submitButton.disabled = true;
      submitButton.textContent = "Nahrávám…";
    }

    const formData = new FormData();
    selectedFiles.forEach((file) => formData.append("files", file, file.name));

    try {
      const data = await fetchJson(api("/api/imports"), {
        method: "POST",
        body: formData,
      });
      saveUploadNotice({
        processed_files: data.processed_files || [],
        skipped_files: data.skipped_files || [],
      });
      clearEmployeeDataDirty();
      selectedFiles.length = 0;
      renderSelectedFiles();
      transitionNavigate(api(`/wizard/${data.import_id}/review`));
    } catch (error) {
      alert(`Nepodařilo se nahrát soubory: ${error.message}`);
      if (submitButton) {
        submitButton.disabled = false;
        submitButton.textContent = originalLabel || "Vytvořit přehled z dokumentů";
      }
    }
  });

  const imports = await loadImports();
  renderImportsList(importsList, imports, {
    emptyMessage: "Zatím tu není žádný import. Nahrajte první HTML reporty z Pamica.",
    onSelect: (importId) => transitionNavigate(api(`/wizard/${importId}/review`)),
  });
}

async function initWizardReview() {
  await loadMetadata();
  const data = await loadPreview(state.currentImportId);
  setSummary(data.summary);
  renderPreviewRows(data.preview_rows || [], {
    target: document.getElementById("previewBody"),
    allowCreate: true,
  });
  updateReviewNotice();
}

async function initWizardRecompute() {
  const recomputeButton = document.getElementById("recomputeButton");
  const recomputeResult = document.getElementById("recomputeResult");
  recomputeButton?.addEventListener("click", async () => {
    recomputeButton.disabled = true;
    recomputeButton.textContent = "Přepočítávám…";
    try {
      const data = await fetchJson(api(`/api/imports/${state.currentImportId}/recompute`), { method: "POST" });
      clearEmployeeDataDirty();
      setSummary(data.summary);
      recomputeResult.className = "result-card success";
      recomputeResult.innerHTML = `
        <h3>Přehled je znovu synchronizovaný.</h3>
        <p>Spárováno: <strong>${data.summary?.matched_rows || 0}</strong>, stále chybí: <strong>${data.summary?.missing_rows || 0}</strong>.</p>
        <div class="hero-actions">
          <a class="button primary" href="${BASE_URL}/wizard/${state.currentImportId}/export" data-nav>Pokračovat</a>
          <a class="button ghost" href="${BASE_URL}/wizard/${state.currentImportId}/review" data-nav>Zpět na kontrolu</a>
        </div>
      `;
      installNavigationTransitions();
    } catch (error) {
      recomputeResult.className = "result-card error";
      recomputeResult.innerHTML = `<h3>Přepočet selhal</h3><p>${escapeHtml(error.message)}</p>`;
    } finally {
      recomputeButton.disabled = false;
      recomputeButton.textContent = "Přepočítat přehled";
      recomputeResult.classList.remove("hidden");
    }
  });
}

async function initWizardExport() {
  const exportButton = document.getElementById("exportButton");
  const exportPanel = exportButton?.closest(".screen-panel");
  if (!exportButton) return;

  const ensureExportNotice = () => {
    let notice = exportPanel?.querySelector("[data-export-notice]");
    if (!notice) {
      notice = document.createElement("div");
      notice.dataset.exportNotice = "1";
      exportPanel?.prepend(notice);
    }
    return notice;
  };

  if (isEmployeeDataDirty()) {
    exportButton.disabled = true;
    const warning = ensureExportNotice();
    warning.className = "notice warning";
    warning.innerHTML = `Než stáhnete XLSX, spusťte nejprve <a href="${BASE_URL}/wizard/${state.currentImportId}/recompute" data-nav>přepočet přehledu</a>.`;
    installNavigationTransitions();
    return;
  }

  await loadPreview(state.currentImportId);
  if (!state.exportReady) {
    exportButton.disabled = true;
    const warning = ensureExportNotice();
    warning.className = "notice warning";
    warning.innerHTML = `Export je zatím blokovaný, protože import není kompletní.<br>${renderCoverageWarnings(state.incompleteGroups)}`;
    return;
  }

  exportButton.addEventListener("click", async () => {
    exportButton.disabled = true;
    const originalLabel = exportButton.textContent;
    exportButton.textContent = "Připravuji…";
    try {
      const data = await fetchJson(api(`/api/exports/${state.currentImportId}`), { method: "POST" });
      const urls = Object.values(data.download_urls || {});
      if (!urls.length) {
        alert("Export je prázdný – žádné řádky ke stažení.");
        return;
      }
      urls.forEach((url, index) => {
        setTimeout(() => {
          const link = document.createElement("a");
          link.href = url;
          link.download = "";
          document.body.appendChild(link);
          link.click();
          link.remove();
        }, index * 300);
      });
    } catch (error) {
      const suffix = error.details?.length ? `\n${error.details.join("\n")}` : "";
      alert(`Nepodařilo se vytvořit export: ${error.message}${suffix}`);
    } finally {
      exportButton.disabled = false;
      exportButton.textContent = originalLabel;
    }
  });
}

async function initEmployeesPage() {
  await loadMetadata();
  await loadEmployees();

  document.getElementById("addEmployeeButton")?.addEventListener("click", () => openEmployeeDialog({ mode: "manual-create" }));

  const searchInput = document.getElementById("employeeSearch");
  const filterProject = document.getElementById("employeeFilterProject");
  const filterCoordinator = document.getElementById("employeeFilterCoordinator");
  const filterCompany = document.getElementById("employeeFilterCompany");
  const resetFilters = document.getElementById("resetEmployeeFilters");

  searchInput?.addEventListener("input", (event) => {
    state.employeeFilters.query = event.target.value || "";
    renderEmployees();
  });
  filterProject?.addEventListener("change", (event) => {
    state.employeeFilters.project = event.target.value || "";
    renderEmployees();
  });
  filterCoordinator?.addEventListener("change", (event) => {
    state.employeeFilters.coordinator = event.target.value || "";
    renderEmployees();
  });
  filterCompany?.addEventListener("change", (event) => {
    state.employeeFilters.company = event.target.value || "";
    renderEmployees();
  });
  resetFilters?.addEventListener("click", () => {
    state.employeeFilters = { query: "", project: "", coordinator: "", company: "" };
    if (searchInput) searchInput.value = "";
    if (filterProject) filterProject.value = "";
    if (filterCoordinator) filterCoordinator.value = "";
    if (filterCompany) filterCompany.value = "";
    renderEmployees();
  });

  const importEmployeesButton = document.getElementById("importEmployeesButton");
  const employeesFileInput = document.getElementById("employeesFileInput");
  importEmployeesButton?.addEventListener("click", () => employeesFileInput?.click());

  document.getElementById("clearEmployeesButton")?.addEventListener("click", async () => {
    if (!confirm("Opravdu smazat všechny zaměstnance z databáze?")) return;
    try {
      const data = await fetchJson(api("/api/employees/clear"), { method: "POST" });
      await loadMetadata();
      state.employees = data.employees || [];
      markEmployeeDataDirty();
      renderEmployees();
      alert(`Hotovo. Smazáno ${data.removed} záznamů.`);
    } catch (error) {
      alert(`Nepodařilo se vymazat databázi: ${error.message}`);
    }
  });

  employeesFileInput?.addEventListener("change", async () => {
    const file = employeesFileInput.files?.[0];
    if (!file) return;
    const formData = new FormData();
    formData.append("file", file, file.name);

    const originalLabel = importEmployeesButton.textContent;
    importEmployeesButton.disabled = true;
    importEmployeesButton.textContent = "Načítám…";
    try {
      const data = await fetchJson(api("/api/employees/import-xlsx"), {
        method: "POST",
        body: formData,
      });
      state.employees = data.employees || [];
      await loadMetadata();
      markEmployeeDataDirty();
      renderEmployees();
      const { created = 0, updated = 0 } = data.stats || {};
      alert(`Hotovo. Nových: ${created}, aktualizovaných: ${updated}, celkem řádků: ${data.total_rows}.`);
    } catch (error) {
      alert(`Nepodařilo se nahrát XLSX: ${error.message}`);
    } finally {
      employeesFileInput.value = "";
      importEmployeesButton.disabled = false;
      importEmployeesButton.textContent = originalLabel;
    }
  });
}

async function initHistoryPage() {
  const importsList = document.getElementById("importsList");
  const historyOpenWizard = document.getElementById("historyOpenWizard");
  const imports = await loadImports();

  async function selectImport(importId) {
    state.currentImportId = importId;
    const data = await loadPreview(importId);
    setSummary(data.summary);
    renderHistoryRows(data.preview_rows || []);
    if (historyOpenWizard) {
      historyOpenWizard.href = api(`/wizard/${importId}/review`);
      historyOpenWizard.classList.remove("disabled-link");
      historyOpenWizard.setAttribute("data-nav", "");
      installNavigationTransitions();
    }
    renderImportsList(importsList, state.imports, {
      activeImportId: importId,
      onSelect: selectImport,
      emptyMessage: "Zatím tu není žádný import.",
    });
  }

  renderImportsList(importsList, imports, {
    activeImportId: state.currentImportId,
    onSelect: selectImport,
    emptyMessage: "Zatím tu není žádný import.",
  });

  if (state.currentImportId) await selectImport(state.currentImportId);
}

async function init() {
  installNavigationTransitions();
  bindEmployeeDialog();

  if (pageId === "wizard" && pageStep === "upload") {
    await initWizardUpload();
  } else if (pageId === "wizard" && pageStep === "review") {
    await initWizardReview();
  } else if (pageId === "wizard" && pageStep === "recompute") {
    await initWizardRecompute();
  } else if (pageId === "wizard" && pageStep === "export") {
    await initWizardExport();
  } else if (pageId === "employees") {
    await initEmployeesPage();
  } else if (pageId === "history") {
    await initHistoryPage();
  }
}

init().catch((error) => {
  console.error(error);
  alert(`Aplikace narazila na chybu: ${error.message}`);
});
