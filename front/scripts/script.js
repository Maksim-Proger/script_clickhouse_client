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
    const fileInput = document.getElementById("fileInput");
    const btnUploadFile = document.getElementById("btnUploadFile");
    const btnConfirmExport = document.querySelector("#exportDialog .primary-button");
    const btnApplyFilters = document.getElementById("btnApplyFilters");

    let exportedData = [];

    Auth.setSessionExpiredHandler(showLogin);

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

    function showLogin() {
        appRoot.classList.add("is-hidden");
        loginPage.classList.remove("is-hidden");
        Auth.logout();
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

    function buildDateTime(dateId, timeId, defaultTime = null) {
        const date = document.getElementById(dateId).value;
        let time = document.getElementById(timeId).value;

        if (!date) return null;

        if (!time && defaultTime) {
            time = defaultTime;
        } else if (!time) {
            return date;
        }

        return `${date} ${time}`;
    }
    async function requestCH() {
        try {
            container.innerHTML = "<p style='padding:20px'>Загрузка...</p>";

            const exactMatch = buildDateTime("filterDate", "filterTime");
            const rangeStart = buildDateTime("filterDateFrom", "filterTimeFrom", "00:00:00");
            const rangeEnd = buildDateTime("filterDateTo", "filterTimeTo", "23:59:59");

            const filters = {
                blocked_at: exactMatch || null,

                period: (rangeStart || rangeEnd) ? {
                    from: rangeStart || null,
                    to: rangeEnd || null
                } : null,

                ip: document.getElementById("filterIP").value.trim() || null,
                source: document.getElementById("filterSource").value || null,
                profile: document.getElementById("filterProfile").value.trim() || null
            };

            const response = await Auth.authFetch(`${Auth.API_BASE}/ch/read`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(filters)
            });

            const result = await response.json();
            exportedData = result.data || result;

            if (!Array.isArray(exportedData)) throw new Error("Некорректный ответ сервера");

            renderTable(exportedData);
        } catch (e) {
            if (e.message !== "Unauthorized") {
                container.innerHTML = `<p style='padding:20px; color:red'>Ошибка: ${e.message}</p>`;
            }
        }
    }

    function renderTable(data) {
        if (!data.length) {
            container.innerHTML = "<p style='padding:20px'>Данные не найдены по заданным фильтрам</p>";
            return;
        }
        const headers = Object.keys(data[0]);
        let html = `<table><thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>`;
        data.forEach(row => {
            html += `<tr>${headers.map(key => `<td>${row[key] ?? ''}</td>`).join('')}</tr>`;
        });
        container.innerHTML = html + "</tbody></table>";
    }

    async function requestDG() {
        try {
            const payload = {
                name: document.getElementById("dgName").value.trim(),
                data: {
                    id: document.getElementById("dgId").value.trim(),
                    value: document.getElementById("dgValue").value.trim(),
                    type: document.getElementById("dgType").value.trim()
                }
            };

            console.log("Sending manual request to DG:", payload);

            const response = await Auth.authFetch(`${Auth.API_BASE}/dg/request`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
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
                        .filter(line => line.length > 6)
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

    function exportData() {
        if (!exportedData?.length) return alert("Нет данных для экспорта. Сначала выполните запрос.");
        const ws = XLSX.utils.json_to_sheet(exportedData);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "BlockedIPs");
        XLSX.writeFile(wb, 'clickhouse_export.xlsx');
        exportDialog.close();
    }

    document.getElementById("btnCH").addEventListener("click", () => rchFilterDialog.showModal());

    btnApplyFilters.addEventListener("click", () => {
        rchFilterDialog.close();
        requestCH();
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
