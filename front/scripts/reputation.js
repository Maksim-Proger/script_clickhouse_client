import * as Auth from './auth.js';
import { requireAuthOrRedirect, initProfilePanel } from './app_shell.js';

const LOGIN_PAGE = '/templates/new_index.html';

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

async function fetchReputation() {
    const response = await Auth.authFetch(`${Auth.API_BASE}/ch/reputation`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({})
    });
    if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${response.status}`);
    }
    const result = await response.json();
    return Array.isArray(result) ? result : (result.data || []);
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

async function load() {
    try {
        const data = await fetchReputation();
        if (data.length && data[0].computed_at) {
            snapshotMeta.textContent = `Снапшот от ${formatDate(data[0].computed_at)}`;
        } else {
            snapshotMeta.textContent = "";
        }
        renderTable(data);
    } catch (e) {
        if (e.message === "Unauthorized") return;
        container.innerHTML = `<p style='padding:20px; color:var(--color-danger)'>Ошибка: ${e.message}</p>`;
        snapshotMeta.textContent = "";
    }
}

load();
