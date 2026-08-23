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

let currentPage = 1;
let currentItemsListId = null;
let searchDebounce = null;
let refreshTimer = null;
const listsById = new Map();

const SOURCE_LABELS = {
    reputation: "Репутация",
    blocked_ips: "CH / blocked_ips",
    manual: "Вручную",
};

function formatDate(s) {
    if (!s) return "-";
    return String(s).split(".")[0].replace("T", " ");
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

        clearTimeout(refreshTimer);
        if (lists.some(l => l.status === "creating" || l.status === "pending_sync")) {
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

    if (!lists.length) {
        container.innerHTML = "<p style='padding:20px'>Списков пока нет</p>";
        return;
    }

    let html = `<table><thead><tr>
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
        let badge;
        if (l.status === "creating") {
            badge = `<span class="badge badge--creating">Создаётся</span>`;
        } else if (l.status === "pending_sync") {
            badge = `<span class="badge badge--creating">Синхронизация</span>`;
        } else if (l.status === "failed") {
            badge = `<span class="badge badge--failed" title="${escapeHtml(l.last_error)}">Ошибка</span>`;
        } else if (l.status === "sync_failed") {
            badge = `<span class="badge badge--failed" title="${escapeHtml(l.last_error)}">Не синхронизирован</span>`;
                } else if (l.status === "active") {
            badge = `<span class="badge badge--active">Активен</span>`;
        } else {
            badge = `<span class="badge badge--inactive">Архив</span>`;
        }

        let actions;
        if (l.status === "creating" || l.status === "pending_sync") {
            actions = "";
        } else if (l.status === "failed") {
            actions = `<button class="btn btn--danger btn--small" onclick="window.deleteList(${l.id})">Удалить</button>`;
        } else if (l.status === "sync_failed") {
            actions = `
                <button class="btn btn--secondary btn--small" onclick="window.retrySync(${l.id})">Повторить</button>
                <button class="btn btn--danger btn--small" onclick="window.deleteList(${l.id})">Удалить</button>`;
        } else {
            const toggleAction = l.status === "active"
                ? `<button class="btn btn--secondary btn--small" onclick="window.setListStatus(${l.id}, 'archived')">Архив</button>`
                : `<button class="btn btn--secondary btn--small" onclick="window.setListStatus(${l.id}, 'active')">Вернуть</button>`;
            const exportActions = l.status === "active"
                ? `<button class="btn btn--secondary btn--small" onclick="window.exportList(${l.id}, 'txt')">TXT</button>
                <button class="btn btn--secondary btn--small" onclick="window.exportList(${l.id}, 'json')">JSON</button>`
                : "";
            actions = `
                <button class="btn btn--secondary btn--small" onclick="window.openListItems(${l.id})">Элементы</button>
                ${exportActions}
                ${toggleAction}
                <button class="btn btn--danger btn--small" onclick="window.deleteList(${l.id})">Удалить</button>`;
        }

        html += `<tr>
            <td>${l.id}</td>
            <td>${escapeHtml(l.name)}</td>
            <td title="${escapeHtml(l.description)}">${escapeHtml(truncate(l.description, 60))}</td>
            <td>${SOURCE_LABELS[l.source_type] || escapeHtml(l.source_type)}</td>
            <td>${badge}</td>
            <td>${l.item_count}</td>
            <td>v${l.version}</td>
            <td>${formatDate(l.updated_at)}</td>
            <td>${escapeHtml(l.created_by)}</td>
            <td><div class="feed-actions">${actions}</div></td>
        </tr>`;
    });

    html += "</tbody></table>";
    container.innerHTML = html;
}

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

window.retrySync = async (listId) => {
    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/${listId}/retry-sync`, {
            method: "POST"
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
        alert(`Список "${created.name}" создан, элементов: ${created.item_count}`);
        createManualDialog.close();
        await loadLists(1);
    } catch (e) {
        if (e.message !== "Unauthorized") alert(`Ошибка: ${e.message}`);
    } finally {
        btn.disabled = false;
        btn.textContent = "Создать";
    }
});

searchInput.addEventListener("input", () => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => loadLists(1), 300);
});

showArchived.addEventListener("change", () => loadLists(1));

loadLists();
