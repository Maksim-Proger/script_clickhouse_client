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

const container = document.getElementById("users-list");
const changePasswordDialog = document.getElementById("changePasswordDialog");

let changePwdUsername = "";

async function loadUsers() {
    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/users/`);
        if (!response.ok) throw new Error("Ошибка загрузки");
        const users = await response.json();
        renderTable(users);
    } catch (e) {
        if (e.message === "Unauthorized") {
            window.location.replace(LOGIN_PAGE);
        } else {
            container.innerHTML = `<p style="color:var(--color-danger)">Ошибка: ${e.message}</p>`;
        }
    }
}

function renderTable(users) {
    if (!users.length) {
        container.innerHTML = "<p>Нет пользователей</p>";
        return;
    }

    let html = `<table><thead><tr>
        <th>ID</th><th>Логин</th><th>Статус</th><th>Дата создания</th><th>Действия</th>
    </tr></thead><tbody>`;

    users.forEach(u => {
        const badge = u.is_active
            ? `<span class="badge badge--active">Активен</span>`
            : `<span class="badge badge--inactive">Деактивирован</span>`;
        const date = u.created_at.split(".")[0].replace("T", " ");

        let actions = "";
        if (u.is_active) {
            actions = `
                <button class="btn btn--secondary btn--small" onclick="window.openChangePassword('${u.username}')">Пароль</button>
                <button class="btn btn--danger btn--small" onclick="window.deactivateUser('${u.username}')">Деактивировать</button>
            `;
        }

        html += `<tr>
            <td>${u.id}</td>
            <td>${u.username}</td>
            <td>${badge}</td>
            <td>${date}</td>
            <td>${actions}</td>
        </tr>`;
    });

    html += "</tbody></table>";
    container.innerHTML = html;
}

window.openChangePassword = (username) => {
    changePwdUsername = username;
    document.getElementById("changePwdTarget").textContent = `Пользователь: ${username}`;
    document.getElementById("newPasswordChange").value = "";
    changePasswordDialog.showModal();
};

document.getElementById("btnConfirmChangePassword").addEventListener("click", async () => {
    const newPassword = document.getElementById("newPasswordChange").value;
    if (!newPassword) return alert("Введите новый пароль");

    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/users/change-password`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username: changePwdUsername, new_password: newPassword })
        });
        if (response.ok) {
            alert("Пароль изменён");
            changePasswordDialog.close();
        } else {
            const err = await response.json();
            alert(`Ошибка: ${err.detail}`);
        }
    } catch (e) {
        if (e.message !== "Unauthorized") alert("Ошибка при смене пароля");
    }
});

window.deactivateUser = async (username) => {
    if (!confirm(`Деактивировать пользователя "${username}"?`)) return;

    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/users/delete`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username })
        });
        if (response.ok) {
            await loadUsers();
        } else {
            const err = await response.json();
            alert(`Ошибка: ${err.detail}`);
        }
    } catch (e) {
        if (e.message !== "Unauthorized") alert("Ошибка при деактивации");
    }
};

document.getElementById("btnCreateUser").addEventListener("click", async () => {
    const username = document.getElementById("newUsername").value.trim();
    const password = document.getElementById("newPassword").value;
    if (!username || !password) return alert("Заполните все поля");

    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/users/create`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username, password })
        });
        if (response.ok) {
            document.getElementById("newUsername").value = "";
            document.getElementById("newPassword").value = "";
            await loadUsers();
        } else {
            const err = await response.json();
            alert(`Ошибка: ${err.detail}`);
        }
    } catch (e) {
        if (e.message !== "Unauthorized") alert("Ошибка при создании пользователя");
    }
});

loadUsers();
