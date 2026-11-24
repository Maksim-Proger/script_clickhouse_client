import * as Auth from './auth.js';

const EXCLUDE_PAGE_SIZE = 50;

export const NAME_CHARS_ERROR = "В названии нельзя использовать символы / ? # %";

export const IP_REGEX = /^(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}$/;

export function hasForbiddenNameChars(name) {
    return /[/?#%\r\n\t]/.test(name);
}

const selections = new WeakMap();

export function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
}

const RISK_LABELS = {
    critical: "критический",
    high: "высокий",
    bad: "плохой",
    suspicious: "подозрительный",
    low: "низкий",
};

export function riskLabel(level) {
    return RISK_LABELS[level] || level;
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

export async function searchIpInLists(ip) {
    const params = new URLSearchParams({ ip });
    const response = await Auth.authFetch(`${Auth.API_BASE}/api/feed-lists/search-ip?${params}`);
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
    resetSelectAll(container);
    getSelection(container).clear();
    container.querySelectorAll(".exclude-list-checkbox").forEach(cb => { cb.checked = false; });
    renderExcludeFooter(container);
}

export function confirmExcludes(container) {
    if (!container || !container.dataset.incomplete) return true;
    if (container.dataset.incomplete === "changed") {
        return confirm("Фид-листы изменились во время загрузки, выбор может быть неточным. Продолжить?");
    }
    const total = Number(container.dataset.total || 0);
    const selected = getSelection(container).size;
    return confirm(`Загрузились не все фид-листы, исключено ${selected} из ${total}. Продолжить?`);
}

export async function renderPresetOptions(select) {
    let presets = [];
    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/ch/reputation/presets`);
        if (response.ok) presets = await response.json();
    } catch (e) {
        if (e.message === "Unauthorized") return;
    }
    select.innerHTML = `<option value="">Общая репутация</option>` + presets.map(p =>
        `<option value="${escapeHtml(p.key)}">${escapeHtml(p.title)}</option>`
    ).join("");
}

export async function renderExcludeOptions(container, appliedIds = []) {
    if (!container) return;
    const selected = getSelection(container);
    selected.clear();
    appliedIds.forEach(id => selected.add(Number(id)));

    if (container.dataset.ready !== "1") {
        container.innerHTML = `
            <input type="text" class="exclude-lists__search" placeholder="Поиск по названию">
            <label class="radio-option">
                <input type="checkbox" class="exclude-lists__all">
                <span>Выбрать все</span>
            </label>
            <div class="exclude-lists__items">Загрузка...</div>
            <div class="exclude-lists__footer"></div>`;
        container.dataset.ready = "1";

        let searchDebounce = null;
        container.querySelector(".exclude-lists__search").addEventListener("input", () => {
            resetSelectAll(container);
            delete container.dataset.notice;
            renderExcludeFooter(container);
            clearTimeout(searchDebounce);
            searchDebounce = setTimeout(() => {
                startLoad(container);
                loadExcludePage(container, 1).catch(() => {});
            }, 300);
        });

        container.querySelector(".exclude-lists__all").addEventListener("change", (e) => {
            if (e.target.checked) {
                selectAllExcludes(container).catch(() => {});
            } else {
                clearExcludeSelection(container);
            }
        });

        container.querySelector(".exclude-lists__items").addEventListener("change", (e) => {
            const checkbox = e.target.closest(".exclude-list-checkbox");
            if (!checkbox) return;
            resetSelectAll(container);
            const selected = getSelection(container);
            if (checkbox.checked) {
                selected.add(Number(checkbox.value));
            } else {
                selected.delete(Number(checkbox.value));
            }
            renderExcludeFooter(container);
        });
    }

    resetSelectAll(container);
    delete container.dataset.notice;
    startLoad(container);
    await loadExcludePage(container, 1);
}

function startLoad(container) {
    container.dataset.load = String(Number(container.dataset.load || 0) + 1);
    return container.dataset.load;
}

function resetSelectAll(container) {
    if (container.dataset.ready !== "1") return;
    if (container.dataset.loading === "1") {
        startLoad(container);
        setLoading(container, false);
    }
    container.querySelector(".exclude-lists__all").checked = false;
    delete container.dataset.incomplete;
}

function setLoading(container, loading) {
    const button = container.closest("dialog")?.querySelector(".primary-button");
    container.querySelector(".exclude-lists__search").disabled = loading;
    if (loading) {
        container.dataset.loading = "1";
        if (button) {
            button.dataset.text = button.textContent;
            button.dataset.disabled = button.disabled ? "1" : "";
            button.style.minWidth = `${button.getBoundingClientRect().width}px`;
            button.textContent = "Загрузка...";
            button.disabled = true;
        }
    } else if (container.dataset.loading === "1") {
        delete container.dataset.loading;
        if (button) {
            button.textContent = button.dataset.text;
            button.disabled = button.dataset.disabled === "1";
            button.style.minWidth = "";
        }
    }
}

async function selectAllExcludes(container) {
    resetSelectAll(container);
    container.querySelector(".exclude-lists__all").checked = true;
    const load = startLoad(container);
    const selected = getSelection(container);
    selected.clear();
    setLoading(container, true);

    let loaded = true;
    let total = null;
    for (let page = 1; loaded && page <= Number(container.dataset.totalPages || 1); page++) {
        loaded = await loadExcludePage(container, page);
        if (container.dataset.load !== load) return;
        if (total === null) total = container.dataset.total;
    }

    container.querySelectorAll(".exclude-list-checkbox").forEach(cb => {
        cb.checked = true;
        selected.add(Number(cb.value));
    });
    if (!loaded) {
        container.dataset.incomplete = "1";
    } else if (container.dataset.total !== total || selected.size !== Number(total)) {
        container.dataset.incomplete = "changed";
    }
    setLoading(container, false);
    renderExcludeFooter(container);
}

async function loadExcludePage(container, page) {
    const items = container.querySelector(".exclude-lists__items");
    const search = container.querySelector(".exclude-lists__search").value.trim();
    const load = container.dataset.load;

    if (page === 1 && container.dataset.loading !== "1") items.innerHTML = "Загрузка...";

    try {
        const result = await fetchFeedLists({ search, status: "active", page, pageSize: EXCLUDE_PAGE_SIZE });
        if (container.dataset.load !== load) return false;
        const lists = (result.data || []).filter(l =>
            page === 1 || !items.querySelector(`.exclude-list-checkbox[value="${l.id}"]`));
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
        delete container.dataset.notice;
        renderExcludeFooter(container);
        return true;
    } catch (e) {
        if (e.message === "Unauthorized") throw e;
        if (container.dataset.load !== load) return false;
        if (page === 1) {
            items.innerHTML = `<p class="exclude-lists__empty">Не удалось загрузить списки</p>`;
        } else {
            container.dataset.notice = "Не удалось загрузить списки. Попробуйте ещё раз.";
            renderExcludeFooter(container);
        }
        return false;
    }
}

function renderExcludeFooter(container) {
    const footer = container.querySelector(".exclude-lists__footer");
    if (!footer) return;

    const page = Number(container.dataset.page || 1);
    const totalPages = Number(container.dataset.totalPages || 1);
    const total = Number(container.dataset.total || 0);
    const selectedCount = getSelection(container).size;
    const selectAll = container.querySelector(".exclude-lists__all");
    selectAll.disabled = !selectAll.checked && total === 0;

    const more = page < totalPages && container.dataset.loading !== "1"
        ? `<button type="button" class="exclude-lists__more">Показать ещё</button>`
        : "";

    let notice = container.dataset.notice || "";
    if (container.dataset.incomplete === "1") {
        notice = `Загрузились не все списки, выбрано ${selectedCount} из ${total}. `
            + "Снимите и поставьте галочку ещё раз";
    } else if (container.dataset.incomplete === "changed") {
        notice = "Пока шла загрузка, списки изменились. Снимите и поставьте галочку ещё раз";
    }

    footer.innerHTML = `<span>Активных списков: ${total}, выбрано: ${selectedCount}</span>${more}`
        + (notice ? `<span class="exclude-lists__notice">${notice}</span>` : "");
    footer.querySelector(".exclude-lists__more")
        ?.addEventListener("click", (e) => {
            e.target.disabled = true;
            loadExcludePage(container, page + 1).catch(() => {});
        });
}
