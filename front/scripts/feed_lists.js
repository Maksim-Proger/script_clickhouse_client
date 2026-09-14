import * as Auth from './auth.js';
import { requireAuthOrRedirect, initProfilePanel } from './app_shell.js';
import { fetchFeedLists, createFeedList, escapeHtml } from './feed_lists_api.js';

const LOGIN_PAGE = '/templates/new_index.html';
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

const container = document.getElementById("feed-lists");
const paginationContainer = document.getElementById("pagination-container");
const searchInput = document.getElementById("feedSearch");
const showArchived = document.getElementById("feedShowArchived");

const itemsDialog = document.getElementById("feedItemsDialog");
const itemsTitle = document.getElementById("feedItemsTitle");
const itemsMeta = document.getElementById("feedItemsMeta");
const itemsTable = document.getElementById("feedItemsTable");
const itemsPagination = document.getElementById("feedItemsPagination");

const createManualDialog = document.getElementById("createManualDialog");

const appendDialog = document.getElementById("appendDialog");
const appendTitle = document.getElementById("appendTitle");
const btnConfirmAppend = document.getElementById("btnConfirmAppend");

let currentPage = 1;
let currentItemsListId = null;
let appendListId = null;
let searchDebounce = null;
let refreshTimer = null;
const listsById = new Map();
const versionsByList = new Map();
const expandedIds = new Set();

const BUILD_KIND_LABELS = {
    create: "создание",
    append: "дополнение",
    restore: "восстановление",
};

const SOURCE_LABELS = {
    reputation: "Репутация",
    blocked_ips: "CH / blocked_ips",
    manual: "Вручную",
};

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

async function loadLists(page = 1) {
    currentPage = page;
    try {
        container.innerHTML = "<p style='padding:20px'>Загрузка...</p>";
        paginationContainer.innerHTML = "";

        const result = await fetchFeedLists({
            search: searchInput.value.trim(),
            status: "",
            page,
            pageSize: PAGE_SIZE,
        });

        let lists = result.data || [];
        if (!showArchived.checked) {
            lists = lists.filter(l => l.status !== "archived");
        }

        renderTable(lists);
        renderPagination(result.page || 1, result.total_pages || 1);
        refreshExpanded();

        clearTimeout(refreshTimer);
        if (lists.some(l => l.pending)) {
            refreshTimer = setTimeout(() => loadLists(currentPage), 4000);
        }
    } catch (e) {
        if (e.message === "Unauthorized") return;
        container.innerHTML = `<p style='padding:20px; color:var(--color-danger)'>Ошибка: ${escapeHtml(e.message)}</p>`;
    }
}

function renderTable(lists) {
    listsById.clear();
    lists.forEach(l => listsById.set(l.id, l));

    expandedIds.forEach(id => {
        if (!listsById.has(id)) expandedIds.delete(id);
    });
    [...versionsByList.keys()].forEach(id => {
        if (!listsById.has(id)) versionsByList.delete(id);
    });

    if (!lists.length) {
        container.innerHTML = "<p style='padding:20px'>Списков пока нет</p>";
        return;
    }

    let html = `<table><thead><tr>
        <th></th>
        <th>ID</th>
        <th>Название</th>
        <th>Описание</th>
        <th>Источник</th>
        <th>Статус</th>
        <th>Элементов</th>
        <th>Версия</th>
        <th>Обновлён</th>
        <th>Создал</th>
        <th>Действия</th>
    </tr></thead><tbody>`;

    lists.forEach(l => {
        const toggle = l.has_history
            ? `<button class="btn btn--secondary btn--small" onclick="window.toggleHistory(${l.id})">${expandedIds.has(l.id) ? "▴" : "▾"}</button>`
            : "";

        html += `<tr>
            <td>${toggle}</td>
            <td>${l.id}</td>
            <td>${escapeHtml(l.name)}</td>
            <td title="${escapeHtml(l.description)}">${escapeHtml(truncate(l.description, 60))}</td>
            <td>${SOURCE_LABELS[l.source_type] || escapeHtml(l.source_type)}</td>
            <td>${renderBadge(l)}</td>
            <td>${l.item_count}</td>
            <td>${l.current_version ? "v" + l.current_version : "-"}</td>
            <td>${formatDate(l.updated_at)}</td>
            <td>${escapeHtml(l.created_by)}</td>
            <td><div class="feed-actions">${renderActions(l)}</div></td>
        </tr>`;

        if (expandedIds.has(l.id)) {
            html += `<tr class="feed-history-row"><td colspan="11" id="feed-history-${l.id}">Загрузка...</td></tr>`;
        }
    });

    html += "</tbody></table>";
    container.innerHTML = html;
}

function renderBadge(l) {
    if (l.pending) {
        return `<span class="badge badge--creating">Обновляется</span>`;
    }
    if (!l.current_version) {
        return `<span class="badge badge--failed" title="${escapeHtml(l.last_error)}">Ошибка</span>`;
    }
    if (l.status === "active") {
        return `<span class="badge badge--active" title="${escapeHtml(l.last_error)}">Активен</span>`;
    }
    return `<span class="badge badge--inactive">Архив</span>`;
}

function renderActions(l) {
    const lock = l.busy ? "disabled" : "";

    if (!l.current_version) {
        return l.busy
            ? ""
            : `<button class="btn btn--danger btn--small" onclick="window.deleteList(${l.id})">Удалить</button>`;
    }

    const toggleAction = l.status === "active"
        ? `<button class="btn btn--secondary btn--small" onclick="window.setListStatus(${l.id}, 'archived')" ${lock}>Архив</button>`
        : `<button class="btn btn--secondary btn--small" onclick="window.setListStatus(${l.id}, 'active')" ${lock}>Вернуть</button>`;

    const exportActions = l.status === "active"
        ? `<button class="btn btn--secondary btn--small" onclick="window.exportList(${l.id}, 'txt')">TXT</button>
            <button class="btn btn--secondary btn--small" onclick="window.exportList(${l.id}, 'json')">JSON</button>`
        : "";

    const changeAction = l.status === "active"
        ? `<button class="btn btn--secondary btn--small" onclick="window.openAppendDialog(${l.id})" ${lock}>Изменить</button>`
        : "";

    return `
        <button class="btn btn--secondary btn--small" onclick="window.openListItems(${l.id})">Элементы</button>
        ${exportActions}
        ${changeAction}
        ${toggleAction}
        <button class="btn btn--danger btn--small" onclick="window.deleteList(${l.id})" ${lock}>Удалить</button>`;
}

function renderHistory(listId) {
    const cell = document.getElementById(`feed-history-${listId}`);
    if (!cell) return;

    const list = listsById.get(listId);
    const cached = versionsByList.get(listId);

    if (!cached) {
        cell.textContent = "Загрузка...";
        return;
    }
    if (!cached.data.length) {
        cell.textContent = "Прошлых версий нет";
        return;
    }

    const lock = list?.busy || list?.status !== "active" ? "disabled" : "";
    const rows = cached.data.map(v => `<tr>
        <td>v${v.version}</td>
        <td>${formatDate(v.created_at)}</td>
        <td>${v.item_count}</td>
        <td>${escapeHtml(v.created_by)}</td>
        <td>${BUILD_KIND_LABELS[v.build_kind] || escapeHtml(v.build_kind)}</td>
        <td><button class="btn btn--secondary btn--small" onclick="window.restoreVersion(${listId}, ${v.version})" ${lock}>Восстановить</button></td>
    </tr>`).join("");

    cell.innerHTML = `<div class="feed-history"><table><thead><tr>
        <th>Версия</th><th>Создана</th><th>Элементов</th><th>Создал</th><th>Тип</th><th></th>
    </tr></thead><tbody>${rows}</tbody></table></div>`;
}

async function loadVersions(listId) {
    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/${listId}/versions`);
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        const result = await response.json();
        versionsByList.set(listId, {
            forVersion: result.current_version,
            data: result.data || [],
        });
        renderHistory(listId);
    } catch (e) {
        if (e.message === "Unauthorized") return;
        const cell = document.getElementById(`feed-history-${listId}`);
        if (cell) cell.innerHTML = `<span style="color:var(--color-danger)">Ошибка: ${escapeHtml(e.message)}</span>`;
    }
}

function refreshExpanded() {
    expandedIds.forEach(listId => {
        const list = listsById.get(listId);
        const cached = versionsByList.get(listId);
        if (cached && list && cached.forVersion === list.current_version) {
            renderHistory(listId);
        } else {
            versionsByList.delete(listId);
            renderHistory(listId);
            loadVersions(listId);
        }
    });
}

window.toggleHistory = (listId) => {
    if (expandedIds.has(listId)) {
        expandedIds.delete(listId);
    } else {
        expandedIds.add(listId);
    }
    renderTable([...listsById.values()]);
    refreshExpanded();
};

function truncate(s, max) {
    const str = String(s ?? "");
    return str.length > max ? str.slice(0, max) + "..." : str;
}

function renderItemScore(score) {
    if (score === null || score === undefined) return "-";
    const pct = Math.max(0, Math.min(100, score));
    return `<div class="score-cell">
        <span class="score-cell__value">${score.toFixed(1)}</span>
        <div class="score-cell__bar"><div class="score-cell__fill" style="width:${pct}%"></div></div>
    </div>`;
}

function renderItemRisk(level) {
    if (!level) return "-";
    const known = ["suspicious", "bad", "high", "critical"];
    const cls = known.includes(level) ? `risk--${level}` : "risk--suspicious";
    return `<span class="risk ${cls}">${escapeHtml(level)}</span>`;
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

    pag.querySelector("#btnPrevPage")?.addEventListener("click", () => loadLists(page - 1));
    pag.querySelector("#btnNextPage")?.addEventListener("click", () => loadLists(page + 1));
}

window.openListItems = async (listId) => {
    currentItemsListId = listId;
    const name = listsById.get(listId)?.name ?? `#${listId}`;
    itemsTitle.textContent = `Элементы списка "${name}"`;
    itemsMeta.textContent = "";
    itemsTable.innerHTML = "Загрузка...";
    itemsPagination.innerHTML = "";
    itemsDialog.showModal();
    await loadItems(1);
};

async function loadItems(page) {
    try {
        const response = await Auth.authFetch(
            `${Auth.API_BASE}/api/feed-lists/${currentItemsListId}/items?page=${page}&page_size=${ITEMS_PAGE_SIZE}`
        );
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        const result = await response.json();
        const data = result.data || [];

        itemsMeta.textContent =
            `Всего: ${result.total} | версия v${result.version} | обновлён ${formatDate(result.updated_at)}`;

        if (!data.length) {
            itemsTable.innerHTML = "<p>Список пуст</p>";
            itemsPagination.innerHTML = "";
            return;
        }

        let html = `<table><thead><tr>
            <th>Значение</th><th>Тип</th><th>Score</th><th>Риск</th>
            <th>ASN</th><th>Страна</th><th>Источник</th>
            <th>Впервые</th><th>Последний раз</th>
        </tr></thead><tbody>`;

        data.forEach(item => {
            html += `<tr>
                <td>${escapeHtml(item.value)}</td>
                <td>${escapeHtml(item.value_type)}</td>
                <td>${renderItemScore(item.score)}</td>
                <td>${renderItemRisk(item.risk_level)}</td>
                <td>${item.asn ? "AS" + item.asn : "-"}</td>
                <td>${escapeHtml(item.country) || "-"}</td>
                <td>${escapeHtml(item.source) || "-"}</td>
                <td>${formatDate(item.first_seen)}</td>
                <td>${formatDate(item.last_seen)}</td>
            </tr>`;
        });

        html += "</tbody></table>";
        itemsTable.innerHTML = html;

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

window.exportList = async (listId, format) => {
    try {
        const response = await Auth.authFetch(
            `${Auth.API_BASE}/api/feed-lists/${listId}/export?format=${format}`
        );
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        const blob = await response.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `feed_list_${listId}.${format}`;
        a.click();
        URL.revokeObjectURL(url);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка экспорта: ${e.message}`);
    }
};

window.setListStatus = async (listId, status) => {
    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/${listId}/status`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ status })
        });
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        await loadLists(currentPage);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка: ${e.message}`);
    }
};

window.deleteList = async (listId) => {
    const name = listsById.get(listId)?.name ?? `#${listId}`;
    if (!confirm(`Удалить список "${name}" безвозвратно? Сервисы, использующие его, перестанут получать данные.`)) return;

    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/${listId}`, {
            method: "DELETE"
        });
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        await loadLists(currentPage);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка удаления: ${e.message}`);
    }
};

window.restoreVersion = async (listId, version) => {
    const list = listsById.get(listId);
    const cached = versionsByList.get(listId);
    if (!list || !cached) return;

    const target = cached.data.find(v => v.version === version);
    if (!target) {
        alert("Список версий устарел, обновляю");
        versionsByList.delete(listId);
        refreshExpanded();
        return;
    }

    const dropped = [
        ...cached.data.filter(v => v.version > version).map(v => v.version),
        list.current_version,
    ].sort((a, b) => a - b);

    const droppedText = dropped.length === 1
        ? `версия ${dropped[0]} будет удалена безвозвратно`
        : `версии ${dropped.join(", ")} будут удалены безвозвратно`;

    const text = `Версия ${version} станет актуальной, в ней ${target.item_count} адресов вместо текущих ${list.item_count}. `
        + `Кроме того, ${droppedText}.`
        + `\n\nВосстановить?`;

    if (!confirm(text)) return;

    try {
        const response = await Auth.authFetch(
            `${Auth.API_BASE}/api/feed-lists/${listId}/versions/${version}/restore`,
            { method: "POST" }
        );
        const result = await response.json().catch(() => ({}));

        if (response.status === 409) {
            alert(result.detail || "Над списком уже идёт операция");
            await loadLists(currentPage);
            return;
        }
        if (!response.ok) {
            throw new Error(errorText(result.detail, response.status));
        }

        listsById.set(listId, result);
        versionsByList.delete(listId);
        renderTable([...listsById.values()]);
        refreshExpanded();

        clearTimeout(refreshTimer);
        refreshTimer = setTimeout(() => loadLists(currentPage), 4000);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка восстановления: ${e.message}`);
    }
};

const APPEND_SOURCES = ["manual", "blocked_ips", "reputation"];

function appendFilterValue(id) {
    return document.getElementById(id).value.trim();
}

function fillAppendForm(source, filters) {
    const known = APPEND_SOURCES.includes(source) ? source : "manual";

    APPEND_SOURCES.forEach(name => {
        document.getElementById(`appendBlock_${name}`).classList.toggle("is-hidden", name !== known);
    });
    document.querySelector(`input[name="appendSource"][value="${known}"]`).checked = true;

    const f = filters || {};
    document.getElementById("appendManualValues").value = "";

    document.getElementById("appendChDateFrom").value = (f.period?.from ?? f.period?.from_date ?? "").split(" ")[0];
    document.getElementById("appendChDateTo").value = (f.period?.to ?? f.period?.to_date ?? "").split(" ")[0];
    document.getElementById("appendChIp").value = f.ip || "";
    document.getElementById("appendChSource").value = f.source || "";
    document.getElementById("appendChProfile").value = f.profile || "";

    document.getElementById("appendRepScoreFrom").value = f.score_from ?? "";
    document.getElementById("appendRepScoreTo").value = f.score_to ?? "";
    document.getElementById("appendRepIp").value = f.ip || "";
    document.getElementById("appendRepAsn").value = (f.asn || []).join(", ");
    document.getElementById("appendRepAsnExclude").checked = !!f.asn_exclude;
    document.getElementById("appendRepCountries").value = (f.country || []).join(", ");
    document.getElementById("appendRepCountryExclude").checked = !!f.country_exclude;
}

function collectAppendPayload() {
    const source = document.querySelector('input[name="appendSource"]:checked').value;

    const payload = {
        source,
        values: null,
        blocked_ips_filters: null,
        reputation_filters: null,
    };

    if (source === "manual") {
        const values = document.getElementById("appendManualValues").value
            .split("\n").map(v => v.trim()).filter(Boolean);
        if (!values.length) throw new Error("Добавьте хотя бы одно значение");
        payload.values = values;
        return payload;
    }

    if (source === "blocked_ips") {
        const filters = {};
        const from = appendFilterValue("appendChDateFrom");
        const to = appendFilterValue("appendChDateTo");
        if (from || to) {
            filters.period = {};
            if (from) filters.period.from = `${from} 00:00:00`;
            if (to) filters.period.to = `${to} 23:59:59`;
        }
        if (appendFilterValue("appendChIp")) filters.ip = appendFilterValue("appendChIp");
        if (appendFilterValue("appendChSource")) filters.source = appendFilterValue("appendChSource");
        if (appendFilterValue("appendChProfile")) filters.profile = appendFilterValue("appendChProfile");
        payload.blocked_ips_filters = filters;
        return payload;
    }

    const parseList = value => value.split(",").map(s => s.trim()).filter(Boolean);

    const filters = {};
    const scoreFrom = appendFilterValue("appendRepScoreFrom");
    const scoreTo = appendFilterValue("appendRepScoreTo");
    if (scoreFrom) filters.score_from = Number(scoreFrom);
    if (scoreTo) filters.score_to = Number(scoreTo);
    if (appendFilterValue("appendRepIp")) filters.ip = appendFilterValue("appendRepIp");

    const asn = parseList(appendFilterValue("appendRepAsn")).map(Number).filter(Number.isFinite);
    if (asn.length) {
        filters.asn = asn;
        filters.asn_exclude = document.getElementById("appendRepAsnExclude").checked;
    }

    const country = parseList(appendFilterValue("appendRepCountries")).map(c => c.toUpperCase());
    if (country.length) {
        filters.country = country;
        filters.country_exclude = document.getElementById("appendRepCountryExclude").checked;
    }

    payload.reputation_filters = filters;
    return payload;
}

window.openAppendDialog = async (listId) => {
    appendListId = listId;
    appendTitle.textContent = `Изменение списка "${listsById.get(listId)?.name ?? "#" + listId}"`;
    fillAppendForm("manual", null);
    appendDialog.showModal();

    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/${listId}`);
        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || `HTTP ${response.status}`);
        }
        const card = await response.json();
        const applied = card.source_filters;
        if (applied && applied.source) {
            fillAppendForm(applied.source, applied.filters);
        }
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Не удалось загрузить фильтры списка: ${e.message}`);
    }
};

async function submitAppend() {
    let payload;
    try {
        payload = collectAppendPayload();
    } catch (e) {
        return alert(e.message);
    }

    btnConfirmAppend.disabled = true;
    btnConfirmAppend.textContent = "Добавление...";

    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/${appendListId}/append`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const result = await response.json().catch(() => ({}));

        if (response.status === 409) {
            alert(result.detail || "Над списком уже идёт операция");
            return;
        }
        if (!response.ok) {
            throw new Error(errorText(result.detail, response.status));
        }

        listsById.set(appendListId, result);
        appendDialog.close();
        renderTable([...listsById.values()]);
        refreshExpanded();

        clearTimeout(refreshTimer);
        refreshTimer = setTimeout(() => loadLists(currentPage), 4000);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка: ${e.message}`);
    } finally {
        btnConfirmAppend.disabled = false;
        btnConfirmAppend.textContent = "Добавить";
    }
}

document.getElementById("btnCreateManual").addEventListener("click", () => {
    document.getElementById("manualListName").value = "";
    document.getElementById("manualListDescription").value = "";
    document.getElementById("manualListValues").value = "";
    createManualDialog.showModal();
});

document.getElementById("btnConfirmCreateManual").addEventListener("click", async () => {
    const name = document.getElementById("manualListName").value.trim();
    const description = document.getElementById("manualListDescription").value.trim();
    const values = document.getElementById("manualListValues").value
        .split("\n").map(s => s.trim()).filter(Boolean);

    if (!name) return alert("Введите название списка");
    if (!values.length) return alert("Добавьте хотя бы одно значение");

    const btn = document.getElementById("btnConfirmCreateManual");
    btn.disabled = true;
    btn.textContent = "Создание...";

    try {
        const created = await createFeedList({
            name,
            description,
            source_type: "manual",
            values,
        });
        alert(`Список "${created.name}" создаётся, статус можно смотреть в каталоге`);
        createManualDialog.close();
        await loadLists(1);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка: ${e.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = "Создать";
    }
});

document.querySelectorAll('input[name="appendSource"]').forEach(radio => {
    radio.addEventListener("change", () => {
        APPEND_SOURCES.forEach(name => {
            document.getElementById(`appendBlock_${name}`).classList.toggle("is-hidden", name !== radio.value);
        });
    });
});

btnConfirmAppend.addEventListener("click", submitAppend);

searchInput.addEventListener("input", () => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => loadLists(1), 300);
});

showArchived.addEventListener("change", () => loadLists(1));

loadLists();
