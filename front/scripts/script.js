import * as Auth from './auth.js';

document.addEventListener("DOMContentLoaded", () => {

    const loginForm = document.querySelector(".login-form");
    const loginPage = document.querySelector(".login-page");
    const appRoot = document.querySelector(".app");
    const profileBtn = document.getElementById("profileBtn");
    const profileMenu = document.getElementById("profileMenu");
    const btnLogout = document.getElementById("btnLogout");
    const currentUserSpan = document.getElementById("currentUser");
    const dgFilterDialog = document.getElementById("dgFilterDialog");
    const btnApplyDGFilters = document.getElementById("btnApplyDGFilters");
    const uploadDialog = document.getElementById("uploadDialog");
    const exportDialog = document.getElementById("exportDialog");
    const rchFilterDialog = document.getElementById("rchFilterDialog");
    const container = document.getElementById("data-list");
    const paginationContainer = document.getElementById("pagination-container");
    const fileInput = document.getElementById("fileInput");
    const btnUploadFile = document.getElementById("btnUploadFile");
    const btnConfirmExport = document.getElementById("btnConfirmExport");
    const btnApplyFilters = document.getElementById("btnApplyFilters");

    const IP_REGEX = /\b(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}\b/;

    let currentFilters = {};
    let currentPage = 1;

    Auth.setSessionExpiredHandler(showLogin);

    function validateExportPeriod() {
        const fromDate = document.getElementById("exportFilterDateFrom").value;
        const toDate = document.getElementById("exportFilterDateTo").value;
        const warning = document.getElementById("exportPeriodWarning");
        const confirmBtn = document.getElementById("btnConfirmExport");

        if (fromDate && toDate) {
            const from = new Date(fromDate);
            const to = new Date(toDate);
            const diffDays = (to - from) / (1000 * 60 * 60 * 24);
            if (diffDays > 7) {
                warning.style.display = "block";
                confirmBtn.disabled = true;
                return false;
            }
        }
        warning.style.display = "none";
        confirmBtn.disabled = false;
        return true;
    }

    ["exportFilterDateFrom", "exportFilterDateTo",
     "exportFilterTimeFrom", "exportFilterTimeTo"].forEach(id => {
        document.getElementById(id)
            ?.addEventListener("change", validateExportPeriod);
    });

    if (Auth.isAuthenticated()) {
        showApp();
    } else {
        showLogin();
    }

    function showApp() {
        loginPage.classList.add("is-hidden");
        appRoot.classList.remove("is-hidden");
        currentUserSpan.textContent = Auth.getCurrentLogin();
    }

    async function showLogin() {
        appRoot.classList.add("is-hidden");
        loginPage.classList.remove("is-hidden");
        await Auth.logout();
    }

    loginForm.addEventListener("submit", async (e) => {
        e.preventDefault();
        const login = loginForm.elements["login"].value.trim();
        const password = loginForm.elements["password"].value;
        try {
            const success = await Auth.login(login, password);
            if (success) {
                showApp();
                loginForm.reset();
            } else {
                alert("Ошибка: Неверный логин или пароль");
            }
        } catch (err) {
            console.error(err);
            alert("Сервер недоступен");
        }
    });

    btnLogout.addEventListener("click", showLogin);

    profileBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        profileMenu.classList.toggle("is-hidden");
    });
    window.addEventListener("click", () => profileMenu.classList.add("is-hidden"));

    async function requestCH(page = 1) {
        try {
            container.innerHTML = "<p style='padding:20px'>Загрузка...</p>";
            paginationContainer.innerHTML = "";

            const exactDate = document.getElementById("filterDate").value || null;
            const rangeStart = document.getElementById("filterDateFrom").value
                ? document.getElementById("filterDateFrom").value + " " + (document.getElementById("filterTimeFrom").value || "00:00:00")
                : null;
            const rangeEnd = document.getElementById("filterDateTo").value
                ? document.getElementById("filterDateTo").value + " " + (document.getElementById("filterTimeTo").value || "23:59:59")
                : null;

            currentFilters = {
                blocked_at: exactDate,
                period: (rangeStart || rangeEnd) ? {
                    from: rangeStart || null,
                    to: rangeEnd || null
                } : null,
                ip: document.getElementById("filterIP").value.trim() || null,
                source: document.getElementById("filterSource").value || null,
                profile: document.getElementById("filterProfile").value.trim() || null
            };
            currentPage = page;

            const response = await Auth.authFetch(`${Auth.API_BASE}/ch/read`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ ...currentFilters, page, page_size: 100 })
            });

            const result = await response.json();
            const data = result.data || [];

            if (!Array.isArray(data)) throw new Error("Некорректный ответ сервера");

            renderTable(data, result.page || 1, result.total_pages || 1);
        } catch (e) {
            if (e.message !== "Unauthorized") {
                container.innerHTML = `<p style='padding:20px; color:red'>Ошибка: ${e.message}</p>`;
            }
        }
    }

    async function goToPage(page) {
        const dataScreen = document.querySelector('.data-screen');
        if (dataScreen) dataScreen.scrollTop = 0;

        await requestCH(page);
    }

    function renderTable(data, page, totalPages) {
        paginationContainer.innerHTML = "";

        if (!data || !data.length) {
            container.innerHTML = "<p style='padding:20px'>Данные не найдены по заданным фильтрам</p>";
            return;
        }

        const headers = Object.keys(data[0]);
        let html = `<table><thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>`;

        data.forEach(row => {
            html += `<tr>${headers.map(key => `<td>${row[key] ?? ''}</td>`).join('')}</tr>`;
        });

        html += "</tbody></table>";
        container.innerHTML = html;

        if (totalPages > 1) {
            const pag = document.createElement("div");
            pag.className = "pagination";
            pag.innerHTML = `
                <button id="btnPrevPage" ${page <= 1 ? 'disabled' : ''}>← Назад</button>
                <span>Страница ${page} из ${totalPages}</span>
                <button id="btnNextPage" ${page >= totalPages ? 'disabled' : ''}>Вперёд →</button>
            `;
            paginationContainer.appendChild(pag);

            paginationContainer.querySelector("#btnPrevPage")
                ?.addEventListener("click", () => { if (page > 1) goToPage(page - 1); });
            paginationContainer.querySelector("#btnNextPage")
                ?.addEventListener("click", () => { if (page < totalPages) goToPage(page + 1); });
        }
    }

    async function requestDG() {
        try {
            const payload = {
                name: document.getElementById("dgName").value.trim(),
                filter_expired: document.getElementById("dgFilterExpired").checked,
                data: {
                    id: document.getElementById("dgId").value.trim(),
                    value: document.getElementById("dgValue").value.trim(),
                    type: document.getElementById("dgType").value.trim()
                }
            };

            const response = await Auth.authFetch(`${Auth.API_BASE}/dg/request`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });

            if (response.ok) {
                alert(`Запрос для профиля "${payload.name}" успешно отправлен в очередь.`);
            } else {
                const errData = await response.json();
                alert(`Ошибка при отправке запроса: ${errData.detail || response.statusText}`);
            }
        } catch (e) {
            console.error("DG Request Error:", e);
            if (e.message !== "Unauthorized") {
                alert("Не удалось отправить запрос в DG. Проверьте соединение с сервером.");
            }
        }
    }

    async function uploadFile() {
        const file = fileInput.files[0];
        if (!file) return alert("Выберите файл");

        const fileName = file.name.toLowerCase();
        const reader = new FileReader();

        reader.onload = async (e) => {
            try {
                let jsonData = [];
                const now = new Date().toISOString().replace('T', ' ').split('.')[0];

                if (fileName.endsWith('.xlsx')) {
                    const workbook = XLSX.read(new Uint8Array(e.target.result), { type: 'array' });
                    const worksheet = workbook.Sheets[workbook.SheetNames[0]];
                    const rawData = XLSX.utils.sheet_to_json(worksheet);

                    jsonData = rawData.map(row => {
                        const lowerRow = Object.keys(row).reduce((acc, key) => {
                            acc[key.toLowerCase()] = row[key];
                            return acc;
                        }, {});

                        return {
                            blocked_at: lowerRow.blocked_at || now,
                            id: lowerRow.id || null,
                            ip_address: String(lowerRow.ip_address || "").trim(),
                            source: lowerRow.source || "manual_excel",
                            profile: lowerRow.profile || ""
                        };
                    });

                } else if (fileName.endsWith('.txt')) {
                    const text = new TextDecoder().decode(e.target.result);
                    jsonData = text.split('\n')
                        .map(line => line.trim())
                        .filter(line => IP_REGEX.test(line))
                        .map(ip => ({
                            blocked_at: now,
                            id: null,
                            ip_address: ip,
                            source: "manual_txt",
                            profile: ""
                        }));
                }

                const finalData = jsonData.filter(item => item.ip_address.length > 0);

                if (finalData.length === 0) {
                    return alert("В файле не найдено корректных данных");
                }

                const response = await Auth.authFetch(`${Auth.API_BASE}/data/receive`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(finalData)
                });

                if (response.ok) {
                    alert(`Успешно загружено записей: ${finalData.length}`);
                    uploadDialog.close();
                    fileInput.value = "";
                } else {
                    alert("Ошибка при отправке данных на сервер");
                }

            } catch (err) {
                console.error("Ошибка парсинга:", err);
                if (err.message !== "Unauthorized") alert("Ошибка обработки файла");
            }
        };

        reader.readAsArrayBuffer(file);
    }

    async function exportData() {
        if (!validateExportPeriod()) return;

        const exactDate = document.getElementById("exportFilterDate").value || null;
        const fromDate = document.getElementById("exportFilterDateFrom").value;
        const fromTime = document.getElementById("exportFilterTimeFrom").value;
        const toDate = document.getElementById("exportFilterDateTo").value;
        const toTime = document.getElementById("exportFilterTimeTo").value;

        const rangeStart = fromDate ? `${fromDate} ${fromTime || "00:00:00"}` : null;
        const rangeEnd   = toDate   ? `${toDate} ${toTime || "23:59:59"}`    : null;

        const format = document.querySelector('input[name="exportFormat"]:checked').value;
        const uniqueIPs = document.getElementById("exportUniqueIPs").checked;
        const onlyIPField = document.getElementById("exportOnlyIPField").checked;

        const filters = {
            blocked_at: exactDate,
            period: (rangeStart || rangeEnd) ? { from: rangeStart, to: rangeEnd } : null,
            ip:      document.getElementById("exportFilterIP").value.trim()      || null,
            source:  document.getElementById("exportFilterSource").value.trim()  || null,
            profile: document.getElementById("exportFilterProfile").value.trim() || null,
            unique_ips: uniqueIPs,
        };

        const btnConfirmExport = document.getElementById("btnConfirmExport");
        btnConfirmExport.disabled = true;
        btnConfirmExport.textContent = "Загрузка...";

        try {
            const response = await Auth.authFetch(`${Auth.API_BASE}/ch/export`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(filters)
            });

            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.detail || response.statusText);
            }

            const result = await response.json();
            const data = result.data || [];

            if (!data.length) {
                alert("По заданным фильтрам данных не найдено.");
                return;
            }

            const exportRows = onlyIPField
                ? data.map(row => ({ ip_address: row.ip_address }))
                : data;

            if (format === "xlsx") {
                const ws = XLSX.utils.json_to_sheet(exportRows);
                const wb = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(wb, ws, "BlockedIPs");
                XLSX.writeFile(wb, "export.xlsx");
            } else {
                const headers = Object.keys(exportRows[0]).join("\t");
                const rows = exportRows.map(row => Object.values(row).map(v => v ?? "").join("\t"));
                const lines = [headers, ...rows].join("\n");
                const blob = new Blob([lines], { type: "text/plain" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = "export.txt";
                a.click();
                URL.revokeObjectURL(url);
            }

            exportDialog.close();

        } catch (e) {
            if (e.message !== "Unauthorized") {
                alert(`Ошибка экспорта: ${e.message}`);
            }
        } finally {
            btnConfirmExport.disabled = false;
            btnConfirmExport.textContent = "Экспорт";
        }
    }

    document.getElementById("btnCH").addEventListener("click", () => rchFilterDialog.showModal());

    btnApplyFilters.addEventListener("click", () => {
        rchFilterDialog.close();
        requestCH(1);
    });

    document.getElementById("btnDG").addEventListener("click", () => dgFilterDialog.showModal());

    btnApplyDGFilters.addEventListener("click", () => {
        dgFilterDialog.close();
        requestDG();
    });

    document.getElementById("btnUpload").addEventListener("click", () => uploadDialog.showModal());
    btnUploadFile.addEventListener("click", uploadFile);

    document.getElementById("btnExport").addEventListener("click", () => exportDialog.showModal());
    btnConfirmExport.addEventListener("click", exportData);
});