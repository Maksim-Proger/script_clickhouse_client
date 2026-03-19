import * as Auth from './auth.js';

Auth.setSessionExpiredHandler(() => {
    window.location.href = 'new_index.html';
});

if (!Auth.isAuthenticated()) {
    window.location.href = 'new_index.html';
}

const container = document.getElementById("users-list");
const currentUserSpan = document.getElementById("currentUser");
const profileBtn = document.getElementById("profileBtn");
const profileMenu = document.getElementById("profileMenu");
const btnLogout = document.getElementById("btnLogout");
const changePasswordDialog = document.getElementById("changePasswordDialog");

currentUserSpan.textContent = Auth.getCurrentLogin();

profileBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    profileMenu.classList.toggle("is-hidden");
});
window.addEventListener("click", () => profileMenu.classList.add("is-hidden"));

btnLogout.addEventListener("click", async () => {
    await Auth.logout();
    window.location.href = 'new_index.html';
});

let changePwdUsername = "";

async function loadUsers() {
    try {
        const response = await Auth.authFetch(`${Auth.API_BASE}/api/users/`);
        if (!response.ok) throw new Error("Ошибка загрузки");
        const users = await response.json();
        renderTable(users);
    } catch (e) {
        if (e.message === "Unauthorized") {
            window.location.href = 'new_index.html';
        } else {
            container.innerHTML = `<p style="color:red">Ошибка: ${e.message}</p>`;
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
        const statusText = u.is_active ? "Активен" : "Деактивирован";
        const statusColor = u.is_active ? "#16a34a" : "#dc2626";
        const date = u.created_at.split(".")[0].replace("T", " ");

        html += `<tr>
            <td>${u.id}</td>
            <td>${u.username}</td>
            <td style="color:${statusColor}; font-weight:600">${statusText}</td>
            <td>${date}</td>
            <td>`;

        if (u.is_active) {
            html += `<button class="secondary-button" style="padding:5px 10px; font-size:12px; margin-right:5px"
                        onclick="window.openChangePassword('${u.username}')">Пароль</button>`;
            html += `<button class="secondary-button" style="padding:5px 10px; font-size:12px; background:#fee2e2; color:#dc2626"
                        onclick="window.deactivateUser('${u.username}')">Деактивировать</button>`;
        }

        html += `</td></tr>`;
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
