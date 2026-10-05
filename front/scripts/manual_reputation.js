import * as Auth from './auth.js';
import { requireAuthOrRedirect, initProfilePanel } from './app_shell.js';
import { escapeHtml } from './feed_lists_api.js';

const LOGIN_PAGE = '/templates/new_index.html';
const CALCS_URL = `${Auth.API_BASE}/ch/reputation/calcs`;
const PAGE_SIZE = 50;
const ITEMS_PAGE_SIZE = 100;

Auth.setSessionExpiredHandler(() => window.location.replace(LOGIN_PAGE));
requireAuthOrRedirect();

initProfilePanel({
    onLogout: async () => {
        await Auth.logout();
        window.location.replace(LOGIN_PAGE);
    },
});

const container = document.getElementById("calcs");
const paginationContainer = document.getElementById("pagination-container");

const itemsDialog = document.getElementById("calcItemsDialog");
const itemsTitle = document.getElementById("calcItemsTitle");
const itemsMeta = document.getElementById("calcItemsMeta");
const itemsTable = document.getElementById("calcItemsTable");
const itemsPagination = document.getElementById("calcItemsPagination");

const createCalcDialog = document.getElementById("createCalcDialog");
const calcProfile = document.getElementById("calcProfile");

let currentPage = 1;
let currentItemsCalcId = null;
let refreshTimer = null;
const calcsById = new Map();

function formatDate(s) {
    if (!s) return "-";
    return String(s).split(".")[0].replace("T", " ");
}

function errorText(detail, status) {
    if (Array.isArray(detail)) {
        return detail.map(d => d.msg || JSON.stringify(d)).join("; ");
    }
    return detail || `HTTP ${status}`;
}

async function requestJson(url, options = {}) {
    const response = await Auth.authFetch(url, options);
    const result = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(errorText(result.detail, response.status));
    return result;
}

async function loadCalcs(page = 1) {
    currentPage = page;
    try {
        container.innerHTML = "<p style='padding:20px'>Загрузка...</p>";
        paginationContainer.innerHTML = "";

        const result = await requestJson(`${CALCS_URL}?page=${page}&page_size=${PAGE_SIZE}`);
        const calcs = result.data || [];

        renderTable(calcs);
        renderPagination(result.page || 1, result.total_pages || 1);

        clearTimeout(refreshTimer);
        if (calcs.some(c => c.status === "building")) {
            refreshTimer = setTimeout(() => loadCalcs(currentPage), 4000);
        }
    } catch (e) {
        if (e.message === "Unauthorized") return;
        container.innerHTML = `<p style='padding:20px; color:var(--color-danger)'>Ошибка: ${escapeHtml(e.message)}</p>`;
    }
}

function renderTable(calcs) {
    calcsById.clear();
    calcs.forEach(c => calcsById.set(c.id, c));

    if (!calcs.length) {
        container.innerHTML = "<p style='padding:20px'>Расчётов пока нет</p>";
        return;
    }

    let html = `<table><thead><tr>
        <th>ID</th>
        <th>Источник</th>
        <th>Профиль</th>
        <th>Статус</th>
        <th>Строк</th>
        <th>Период</th>
        <th>Создан</th>
        <th>Создал</th>
        <th>Действия</th>
    </tr></thead><tbody>`;

    calcs.forEach(c => {
        const itemsAction = c.status === "ready"
            ? `<button class="btn btn--secondary btn--small" onclick="window.openCalcItems(${c.id})">Элементы</button>`
            : "";

        html += `<tr>
            <td>${c.id}</td>
            <td>${escapeHtml(c.source)}</td>
            <td>${escapeHtml(c.profile) || "все"}</td>
            <td>${renderBadge(c)}</td>
            <td>${c.status === "ready" ? c.row_count : "-"}</td>
            <td>с ${formatDate(c.period_from)}<br>по ${formatDate(c.period_to)}</td>
            <td>${formatDate(c.created_at)}</td>
            <td>${escapeHtml(c.created_by)}</td>
            <td><div class="feed-actions">${itemsAction}</div></td>
        </tr>`;
    });

    html += "</tbody></table>";
    container.innerHTML = html;
}

function renderBadge(c) {
    if (c.status === "building") {
        return `<span class="badge badge--creating">Готовится</span>`;
    }
    if (c.status === "ready") {
        return `<span class="badge badge--active">Готов</span>`;
    }
    return `<span class="badge badge--failed" title="${escapeHtml(c.last_error)}">Ошибка</span>`;
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

    pag.querySelector("#btnPrevPage")?.addEventListener("click", () => loadCalcs(page - 1));
    pag.querySelector("#btnNextPage")?.addEventListener("click", () => loadCalcs(page + 1));
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
    return `<span class="risk ${cls}">${escapeHtml(level)}</span>`;
}

function renderGeo(row) {
    if (!row.country) {
        return `<span class="geo-cell geo-cell--unknown">неизвестно</span>`;
    }
    const city = row.city ? `<div class="geo-cell__city">${escapeHtml(row.city)}</div>` : "";
    return `<div class="geo-cell">
        <div class="geo-cell__country">${escapeHtml(row.country)}</div>
        ${city}
    </div>`;
}

function renderAsn(row) {
    if (!row.asn_number && !row.asn_org) {
        return `<span class="geo-cell--unknown">-</span>`;
    }
    const org = escapeHtml(row.asn_org || "-");
    const num = row.asn_number ? `AS${row.asn_number}` : "";
    return `<div class="asn-cell">
        <div class="asn-cell__org" title="${org}">${org}</div>
        <div class="asn-cell__num">${num}</div>
    </div>`;
}

function renderDetails(row) {
    const items = [
        ["Всего событий", row.events_count],
        ["Макс. за 5 мин", row.max_5m_events],
        ["Макс. за час", row.max_hour_events],
        ["Активных 5-мин окон", row.active_5m_windows],
        ["Активных часов", row.active_hours],
        ["Активных дней", row.active_days],
        ["Впервые замечен", formatDate(row.first_seen)],
    ];
    const cells = items.map(([label, value]) => `
        <div class="details-grid__item">
            <span class="details-grid__label">${label}</span>
            <span class="details-grid__value">${value ?? "-"}</span>
        </div>
    `).join("");
    return `<div class="details-grid">${cells}</div>`;
}

window.openCalcItems = async (calcId) => {
    currentItemsCalcId = calcId;
    const calc = calcsById.get(calcId);
    itemsTitle.textContent = `Расчёт #${calcId}`;
    itemsMeta.textContent = calc
        ? `Источник: ${calc.source} | профиль: ${calc.profile || "все"} | ` +
          `период с ${formatDate(calc.period_from)} по ${formatDate(calc.period_to)} | ` +
          `рассчитан ${formatDate(calc.finished_at)} | строк: ${calc.row_count}`
        : "";
    itemsTable.innerHTML = "Загрузка...";
    itemsPagination.innerHTML = "";
    itemsDialog.showModal();
    await loadItems(1);
};

async function loadItems(page) {
    try {
        const result = await requestJson(
            `${CALCS_URL}/${currentItemsCalcId}/rows?page=${page}&page_size=${ITEMS_PAGE_SIZE}`
        );
        const data = result.data || [];

        if (!data.length) {
            itemsTable.innerHTML = "<p>Подходящих данных нет</p>";
            itemsPagination.innerHTML = "";
            return;
        }

        let html = `<table><thead><tr>
            <th>IP-адрес</th><th>Score</th><th>Риск</th>
            <th>Гео</th><th>ASN</th><th>Последнее событие</th>
        </tr></thead><tbody>`;

        data.forEach((row, idx) => {
            html += `<tr class="clickable" data-idx="${idx}">
                <td>${escapeHtml(row.ip_address)}</td>
                <td>${renderScore(row)}</td>
                <td>${renderRisk(row.risk_level)}</td>
                <td>${renderGeo(row)}</td>
                <td>${renderAsn(row)}</td>
                <td>${formatDate(row.last_seen)}</td>
            </tr>
            <tr class="row-details is-hidden" data-details-for="${idx}">
                <td colspan="6">${renderDetails(row)}</td>
            </tr>`;
        });

        html += "</tbody></table>";
        itemsTable.innerHTML = html;

        itemsTable.querySelectorAll("tr.clickable").forEach(tr => {
            tr.addEventListener("click", () => {
                const details = itemsTable.querySelector(`tr[data-details-for="${tr.dataset.idx}"]`);
                tr.classList.toggle("expanded");
                details.classList.toggle("is-hidden");
            });
        });

        itemsPagination.innerHTML = "";
        if ((result.total_pages || 1) > 1) {
            const pag = document.createElement("div");
            pag.className = "pagination";
            pag.innerHTML = `
                <button id="btnItemsPrev" ${page <= 1 ? 'disabled' : ''}>← Назад</button>
                <span>Страница ${page} из ${result.total_pages}</span>
                <button id="btnItemsNext" ${page >= result.total_pages ? 'disabled' : ''}>Вперёд →</button>
            `;
            itemsPagination.appendChild(pag);
            pag.querySelector("#btnItemsPrev")?.addEventListener("click", () => loadItems(page - 1));
            pag.querySelector("#btnItemsNext")?.addEventListener("click", () => loadItems(page + 1));
        }
    } catch (e) {
        if (e.message === "Unauthorized") return;
        itemsTable.innerHTML = `<p style='color:var(--color-danger)'>Ошибка: ${escapeHtml(e.message)}</p>`;
    }
}

document.getElementById("btnCreateCalc").addEventListener("click", () => {
    document.querySelector('input[name="calcSource"][value="dosgate"]').checked = true;
    calcProfile.value = "";
    createCalcDialog.showModal();
});

document.getElementById("btnConfirmCreateCalc").addEventListener("click", async () => {
    const source = document.querySelector('input[name="calcSource"]:checked').value;
    const profile = calcProfile.value.trim();

    const btn = document.getElementById("btnConfirmCreateCalc");
    btn.disabled = true;
    btn.textContent = "Создание...";

    try {
        await requestJson(CALCS_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(profile ? { source, profile } : { source }),
        });
        createCalcDialog.close();
        await loadCalcs(1);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка: ${e.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = "Создать";
    }
});

loadCalcs();