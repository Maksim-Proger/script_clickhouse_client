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
    const filterDialog = document.getElementById("filterDialog");

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

    async function requestCH() {
        try {
            container.innerHTML = "<p style='padding:20px'>Загрузка...</p>";

            const fDate = document.getElementById("filterDate").value;
            const fIP = document.getElementById("filterIP").value.trim();
            const fSource = document.getElementById("filterSource").value;
            const fProfile = document.getElementById("filterProfile").value.trim();

            let conditions = [];
            if (fDate) conditions.push(`toDate(blocked_at) = '${fDate}'`);
            if (fIP) conditions.push(`ip_address = '${fIP}'`);
            if (fSource) conditions.push(`source = '${fSource}'`);
            if (fProfile) conditions.push(`profile = '${fProfile}'`);

            const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : "";
            const sqlQuery = `SELECT * FROM feedgen.blocked_ips ${whereClause} ORDER BY blocked_at DESC LIMIT 500`;

            const encodedSql = encodeURIComponent(sqlQuery);
            const url = `${Auth.API_BASE}/ch/read?query=${encodedSql}`;

            const response = await Auth.authFetch(url);
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

            const response = await Auth.authFetch(`${Auth.API_BASE}/dg/request`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload)
            });

            if (response.ok) alert("Запрос в DG отправлен с параметрами");
        } catch (e) { console.error(e); }
    }

    async function uploadFile() {
        if (!fileInput.files[0]) return alert("Выберите файл");
        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = async (e) => {
            try {
                const workbook = XLSX.read(new Uint8Array(e.target.result), { type: 'array' });
                const jsonData = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]);
                const response = await Auth.authFetch(`${Auth.API_BASE}/data/receive`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(jsonData)
                });
                if (response.ok) {
                    alert("Данные загружены!");
                    uploadDialog.close();
                    fileInput.value = "";
                } else {
                    alert("Ошибка при загрузке данных на сервер");
                }
            } catch (err) {
                if (err.message !== "Unauthorized") alert("Ошибка обработки Excel-файла");
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

    document.getElementById("btnCH").addEventListener("click", () => filterDialog.showModal());

    btnApplyFilters.addEventListener("click", () => {
        filterDialog.close();
        requestCH();
    });

    document.getElementById("btnDG").addEventListener("click", () => dgFilterDialog.showModal());

    btnApplyDGFilters.addEventListener("click", () => {
        dgFilterDialog.close();
        requestDG();
    });

    document.getElementById("btnUpload").addEventListener("click", () => uploadDialog.showModal());
    document.getElementById("btnExport").addEventListener("click", () => exportDialog.showModal());

    btnUploadFile.addEventListener("click", uploadFile);
    btnConfirmExport.addEventListener("click", exportData);
});
