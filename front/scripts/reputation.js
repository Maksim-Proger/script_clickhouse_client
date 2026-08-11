import * as Auth from './auth.js';
import { requireAuthOrRedirect, initProfilePanel } from './app_shell.js';
import {
    renderExcludeOptions,
    getCheckedExcludeIds,
    clearExcludeSelection,
    createFeedList,
} from './feed_lists_api.js';

const LOGIN_PAGE = '/templates/new_index.html';
const PAGE_SIZE = 100;

Auth.setSessionExpiredHandler(() => window.location.replace(LOGIN_PAGE));
requireAuthOrRedirect();

initProfilePanel({
    onLogout: async () => {
        await Auth.logout();
        window.location.replace(LOGIN_PAGE);
    },
});

const container = document.getElementById("reputation-list");
const snapshotMeta = document.getElementById("snapshotMeta");
const paginationContainer = document.getElementById("pagination-container");

const btnRepFilter = document.getElementById("btnRepFilter");
const btnRepExport = document.getElementById("btnRepExport");
const btnRepSaveList = document.getElementById("btnRepSaveList");
const reputationFilterDialog = document.getElementById("reputationFilterDialog");
const reputationExportDialog = document.getElementById("reputationExportDialog");
const repSaveListDialog = document.getElementById("repSaveListDialog");
const repExcludeLists = document.getElementById("repExcludeLists");

const repScoreFrom = document.getElementById("repScoreFrom");
const repScoreTo = document.getElementById("repScoreTo");
const repIp = document.getElementById("repIp");
const repAsn = document.getElementById("repAsn");
const repAsnExclude = document.getElementById("repAsnExclude");
const repCountry = document.getElementById("repCountry");
const repCountryExclude = document.getElementById("repCountryExclude");

let currentFilters = {};
let currentPage = 1;
let currentSearchId = null;

function parseList(value) {
    return value.split(",").map(s => s.trim()).filter(Boolean);
}

function collectFilters() {
    const f = {};

    if (repScoreFrom.value !== "") f.score_from = parseFloat(repScoreFrom.value);
    if (repScoreTo.value !== "") f.score_to = parseFloat(repScoreTo.value);

    const ip = repIp.value.trim();
    if (ip) f.ip = ip;

    const asn = parseList(repAsn.value).map(Number).filter(Number.isFinite);
    if (asn.length) {
        f.asn = asn;
        f.asn_exclude = repAsnExclude.checked;
    }

    const country = parseList(repCountry.value).map(c => c.toUpperCase());
    if (country.length) {
        f.country = country;
        f.country_exclude = repCountryExclude.checked;
    }

    const excludeIds = getCheckedExcludeIds(repExcludeLists);
    if (excludeIds.length) f.exclude_list_ids = excludeIds;

    return f;
}

function resetFilterFields() {
    [repScoreFrom, repScoreTo, repIp, repAsn, repCountry].forEach(el => { el.value = ""; });
    repAsnExclude.checked = false;
    repCountryExclude.checked = false;
    clearExcludeSelection(repExcludeLists);
}

function updateFilterActive() {
    btnRepFilter.classList.toggle("is-active", Object.keys(currentFilters).length > 0);
}

function postReputation(body) {
    return Auth.authFetch(`${Auth.API_BASE}/ch/reputation`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    });
}

async function fetchPage(page) {
    let response;
    if (currentSearchId) {
        response = await postReputation({ search_id: currentSearchId, page, page_size: PAGE_SIZE });
        if (response.status === 410) {
            currentSearchId = null;
            response = await postReputation({ ...currentFilters, page, page_size: PAGE_SIZE });
        }
    } else {
        response = await postReputation({ ...currentFilters, page, page_size: PAGE_SIZE });
    }

    if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
    }

    const result = await response.json();
    if (Array.isArray(result)) {
        return { data: result, total: result.length, page: 1, total_pages: 1 };
    }
    return result;
}

function formatDate(s) {
    if (!s) return "—";
    return s.split(".")[0].replace("T", " ");
}

function renderGeo(row) {
    if (!row.country) {
        return `<span class="geo-cell geo-cell--unknown">неизвестно</span>`;
    }
    const city = row.city ? `<div class="geo-cell__city">${row.city}</div>` : "";
    return `<div class="geo-cell">
        <div class="geo-cell__country">${row.country}</div>
        ${city}
    </div>`;
}

function renderAsn(row) {
    if (!row.asn_number && !row.asn_org) {
        return `<span class="geo-cell--unknown">—</span>`;
    }
    const org = row.asn_org || "—";
    const num = row.asn_number ? `AS${row.asn_number}` : "";
    return `<div class="asn-cell">
        <div class="asn-cell__org" title="${org}">${org}</div>
        <div class="asn-cell__num">${num}</div>
    </div>`;
}

function renderScore(row) {
    const pct = Math.max(0, Math.min(100, row.score));
    return `<div class="score-cell">
        <span class="score-cell__value">${row.score.toFixed(1)}</span>
        <div class="score-cell__bar"><div class="score-cell__fill" style="width:${pct}%"></div></div>
    </div>`;
}

function renderRisk(level) {
    const known = ["suspicious", "bad", "high", "critical"];
    const cls = known.includes(level) ? `risk--${level}` : "risk--suspicious";
    return `<span class="risk ${cls}">${level}</span>`;
}

function renderDetails(row) {
    const items = [
        ["Всего событий", row.events_count],
        ["Макс. за 5 мин", row.max_5m_events],
        ["Макс. за час", row.max_hour_events],
        ["Активных 5-мин окон", row.active_5m_windows],
        ["Активных часов", row.active_hours],
        ["Активных дней", row.active_days],
        ["Источников", row.sources_count],
        ["Впервые замечен", formatDate(row.first_seen)],
    ];
    const cells = items.map(([label, value]) => `
        <div class="details-grid__item">
            <span class="details-grid__label">${label}</span>
            <span class="details-grid__value">${value ?? "—"}</span>
        </div>
    `).join("");
    return `<div class="details-grid">${cells}</div>`;
}

function renderTable(data) {
    if (!data || !data.length) {
        container.innerHTML = "<p style='padding:20px'>Данные отсутствуют</p>";
        return;
    }

    let html = `<table><thead><tr>
        <th>IP-адрес</th>
        <th>Score</th>
        <th>Риск</th>
        <th>Гео</th>
        <th>ASN</th>
        <th>Источников</th>
        <th>Последнее событие</th>
    </tr></thead><tbody>`;

    data.forEach((row, idx) => {
        html += `<tr class="clickable" data-idx="${idx}">
            <td>${row.ip_address}</td>
            <td>${renderScore(row)}</td>
            <td>${renderRisk(row.risk_level)}</td>
            <td>${renderGeo(row)}</td>
            <td>${renderAsn(row)}</td>
            <td>${row.sources_count}</td>
            <td>${formatDate(row.last_seen)}</td>
        </tr>
        <tr class="row-details is-hidden" data-details-for="${idx}">
            <td colspan="7">${renderDetails(row)}</td>
        </tr>`;
    });

    html += "</tbody></table>";
    container.innerHTML = html;

    container.querySelectorAll("tr.clickable").forEach(tr => {
        tr.addEventListener("click", () => {
            const idx = tr.dataset.idx;
            const details = container.querySelector(`tr[data-details-for="${idx}"]`);
            tr.classList.toggle("expanded");
            details.classList.toggle("is-hidden");
        });
    });
}

function renderPagination(page, totalPages) {
    paginationContainer.innerHTML = "";
    if (totalPages <= 1) return;

    const pag = document.createElement("div");
    pag.className = "pagination";
    pag.innerHTML = `
        <button id="btnPrevPage" ${page <= 1 ? 'disabled' : ''}>← Назад</button>
        <span>Страница ${page} из ${totalPages}</span>
        <button id="btnNextPage" ${page >= totalPages ? 'disabled' : ''}>Вперёд →</button>
    `;
    paginationContainer.appendChild(pag);

    pag.querySelector("#btnPrevPage")
        ?.addEventListener("click", () => { if (page > 1) goToPage(page - 1); });
    pag.querySelector("#btnNextPage")
        ?.addEventListener("click", () => { if (page < totalPages) goToPage(page + 1); });
}

function goToPage(page) {
    const dataScreen = document.querySelector('.data-screen');
    if (dataScreen) dataScreen.scrollTop = 0;
    load(page);
}

async function load(page = 1) {
    try {
        container.innerHTML = "<p style='padding:20px'>Загрузка...</p>";
        paginationContainer.innerHTML = "";

        const result = await fetchPage(page);
        const data = result.data || [];
        currentPage = result.page || page;
        currentSearchId = result.search_id || null;

        if (data.length && data[0].computed_at) {
            snapshotMeta.textContent = `Снапшот от ${formatDate(data[0].computed_at)}`;
        } else {
            snapshotMeta.textContent = "";
        }

        renderTable(data);
        renderPagination(currentPage, result.total_pages || 1);
        btnRepExport.classList.toggle("is-hidden", !(result.total > 0));
        btnRepSaveList.classList.toggle("is-hidden", !(result.total > 0));
    } catch (e) {
        if (e.message === "Unauthorized") return;
        container.innerHTML = `<p style='padding:20px; color:var(--color-danger)'>Ошибка: ${e.message}</p>`;
        snapshotMeta.textContent = "";
    }
}

function postExport(body) {
    return Auth.authFetch(`${Auth.API_BASE}/ch/reputation/export`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    });
}

async function exportReputation() {
    const onlyIp = document.getElementById("repExportOnlyIP").checked;
    const format = document.querySelector('input[name="repExportFormat"]:checked').value;

    const btn = document.getElementById("btnConfirmRepExport");
    btn.disabled = true;
    btn.textContent = "Экспорт...";

    try {
        let response;
        if (currentSearchId) {
            response = await postExport({ search_id: currentSearchId, only_ip: onlyIp });
            if (response.status === 410) {
                currentSearchId = null;
                response = await postExport({ ...currentFilters, only_ip: onlyIp });
            }
        } else {
            response = await postExport({ ...currentFilters, only_ip: onlyIp });
        }

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }

        const result = await response.json();
        const data = result.data || [];

        if (!data.length) {
            alert("Нет данных для экспорта.");
            return;
        }

        if (format === "xlsx") {
            const ws = XLSX.utils.json_to_sheet(data);
            const wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, "Reputation");
            XLSX.writeFile(wb, "reputation.xlsx");
        } else {
            const headers = Object.keys(data[0]).join("\t");
            const rows = data.map(row => Object.values(row).map(v => v ?? "").join("\t"));
            const lines = [headers, ...rows].join("\n");
            const blob = new Blob([lines], { type: "text/plain" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = "reputation.txt";
            a.click();
            URL.revokeObjectURL(url);
        }

        reputationExportDialog.close();
    } catch (e) {
        if (e.message !== "Unauthorized") {
            alert(`Ошибка экспорта: ${e.message}`);
        }
    } finally {
        btn.disabled = false;
        btn.textContent = "Экспорт";
    }
}

btnRepFilter.addEventListener("click", () => {
    reputationFilterDialog.showModal();
    renderExcludeOptions(repExcludeLists).catch(() => {});
});
btnRepExport.addEventListener("click", () => reputationExportDialog.showModal());

btnRepSaveList.addEventListener("click", () => {
    document.getElementById("repListName").value = "";
    document.getElementById("repListDescription").value = "";
    repSaveListDialog.showModal();
});

document.getElementById("btnConfirmRepSaveList").addEventListener("click", async () => {
    const name = document.getElementById("repListName").value.trim();
    const description = document.getElementById("repListDescription").value.trim();
    if (!name) return alert("Введите название списка");

    const btn = document.getElementById("btnConfirmRepSaveList");
    btn.disabled = true;
    btn.textContent = "Сохранение...";

    try {
        const reputationFilters = currentSearchId
            ? { ...currentFilters, search_id: currentSearchId }
            : { ...currentFilters };

        const created = await createFeedList({
            name,
            description,
            source_type: "reputation",
            reputation_filters: reputationFilters,
        });
        alert(`Список "${created.name}" создаётся в фоне, статус можно смотреть в каталоге фид-листов`);
        repSaveListDialog.close();
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка: ${e.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = "Сохранить";
    }
});

document.getElementById("btnApplyRepFilters").addEventListener("click", () => {
    currentFilters = collectFilters();
    currentSearchId = null;
    updateFilterActive();
    reputationFilterDialog.close();
    load(1);
});

document.getElementById("btnResetRepFilters").addEventListener("click", () => {
    resetFilterFields();
    currentFilters = {};
    currentSearchId = null;
    updateFilterActive();
    reputationFilterDialog.close();
    load(1);
});

document.getElementById("btnConfirmRepExport").addEventListener("click", exportReputation);

load();
