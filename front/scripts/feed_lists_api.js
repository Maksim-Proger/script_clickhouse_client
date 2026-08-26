import * as Auth from './auth.js';

const EXCLUDE_PAGE_SIZE = 50;
const selections = new WeakMap();

export function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
}

export async function fetchFeedLists({ search = "", status = "", page = 1, pageSize = EXCLUDE_PAGE_SIZE } = {}) {
    const params = new URLSearchParams({ page, page_size: pageSize });
    if (search) params.set("search", search);
    if (status) params.set("status", status);

    const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/?${params}`);
    if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
    }
    return response.json();
}

export async function createFeedList(payload) {
    const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/create`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
    });
    const result = await response.json().catch(() => ({}));
    if (!response.ok) {
        throw new Error(result.detail || `HTTP ${response.status}`);
    }
    return result;
}

function getSelection(container) {
    let selected = selections.get(container);
    if (!selected) {
        selected = new Set();
        selections.set(container, selected);
    }
    return selected;
}

export function getCheckedExcludeIds(container) {
    if (!container) return [];
    return [...getSelection(container)];
}

export function clearExcludeSelection(container) {
    if (!container) return;
    getSelection(container).clear();
    container.querySelectorAll(".exclude-list-checkbox").forEach(cb => { cb.checked = false; });
    renderExcludeFooter(container);
}

export async function renderExcludeOptions(container, appliedIds = []) {
    if (!container) return;
    const selected = getSelection(container);
    selected.clear();
    appliedIds.forEach(id => selected.add(Number(id)));

    if (container.dataset.ready !== "1") {
        container.innerHTML = `
            <input type="text" class="exclude-lists__search" placeholder="Поиск по названию">
            <div class="exclude-lists__items">Загрузка...</div>
            <div class="exclude-lists__footer"></div>`;
        container.dataset.ready = "1";

        let searchDebounce = null;
        container.querySelector(".exclude-lists__search").addEventListener("input", () => {
            clearTimeout(searchDebounce);
            searchDebounce = setTimeout(() => loadExcludePage(container, 1).catch(() => {}), 300);
        });

        container.querySelector(".exclude-lists__items").addEventListener("change", (e) => {
            const checkbox = e.target.closest(".exclude-list-checkbox");
            if (!checkbox) return;
            const selected = getSelection(container);
            if (checkbox.checked) {
                selected.add(Number(checkbox.value));
            } else {
                selected.delete(Number(checkbox.value));
            }
            renderExcludeFooter(container);
        });
    }

    await loadExcludePage(container, 1);
}

async function loadExcludePage(container, page) {
    const items = container.querySelector(".exclude-lists__items");
    const search = container.querySelector(".exclude-lists__search").value.trim();

    if (page === 1) items.innerHTML = "Загрузка...";

    try {
        const result = await fetchFeedLists({ search, status: "active", page, pageSize: EXCLUDE_PAGE_SIZE });
        const lists = result.data || [];
        const selected = getSelection(container);

        const html = lists.map(l => `
            <label class="radio-option">
                <input type="checkbox" class="exclude-list-checkbox" value="${l.id}"
                       ${selected.has(l.id) ? "checked" : ""}>
                <span>${escapeHtml(l.name)} (${l.item_count})</span>
            </label>
        `).join("");

        if (page === 1) {
            items.innerHTML = html || `<p class="exclude-lists__empty">Активных списков не найдено</p>`;
        } else {
            items.insertAdjacentHTML("beforeend", html);
        }

        container.dataset.page = String(result.page || page);
        container.dataset.totalPages = String(result.total_pages || 1);
        container.dataset.total = String(result.total || 0);
        renderExcludeFooter(container);
    } catch (e) {
        if (e.message === "Unauthorized") throw e;
        if (page === 1) {
            items.innerHTML = `<p class="exclude-lists__empty">Не удалось загрузить списки</p>`;
        }
    }
}

function renderExcludeFooter(container) {
    const footer = container.querySelector(".exclude-lists__footer");
    if (!footer) return;

    const page = Number(container.dataset.page || 1);
    const totalPages = Number(container.dataset.totalPages || 1);
    const total = Number(container.dataset.total || 0);
    const selectedCount = getSelection(container).size;

    const more = page < totalPages
        ? `<button type="button" class="exclude-lists__more">Показать ещё</button>`
        : "";

    footer.innerHTML = `<span>Активных списков: ${total}, выбрано: ${selectedCount}</span>${more}`;
    footer.querySelector(".exclude-lists__more")
        ?.addEventListener("click", () => loadExcludePage(container, page + 1).catch(() => {}));
}
