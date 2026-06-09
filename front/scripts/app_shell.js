import * as Auth from './auth.js';

const LOGIN_PAGE = '/templates/new_index.html';

export function requireAuthOrRedirect() {
    if (!Auth.isAuthenticated()) {
        window.location.replace(LOGIN_PAGE);
        throw new Error("redirecting to login");
    }
}

export function refreshCurrentUser() {
    const span = document.getElementById("currentUser");
    if (span) span.textContent = Auth.getCurrentLogin();
}

export function initProfilePanel({ onLogout } = {}) {
    const profileBtn = document.getElementById("profileBtn");
    const profileMenu = document.getElementById("profileMenu");
    const btnLogout = document.getElementById("btnLogout");
    const btnChangeMyPassword = document.getElementById("btnChangeMyPassword");

    if (!profileBtn || !profileMenu) return { closeMenu: () => {} };

    refreshCurrentUser();

    function closeMenu() {
        profileMenu.classList.add("is-hidden");
        profileBtn.setAttribute("aria-expanded", "false");
    }

    profileBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        const wasOpen = !profileMenu.classList.contains("is-hidden");
        profileMenu.classList.toggle("is-hidden");
        profileBtn.setAttribute("aria-expanded", wasOpen ? "false" : "true");
    });
    window.addEventListener("click", closeMenu);
    profileMenu.addEventListener("click", (e) => e.stopPropagation());

    if (btnLogout && onLogout) {
        btnLogout.addEventListener("click", onLogout);
    }

    if (btnChangeMyPassword) {
        initPasswordDialog(closeMenu);
    }

    return { closeMenu };
}

function initPasswordDialog(closeMenu) {
    const dialog = document.getElementById("changeMyPasswordDialog");
    const newPwd = document.getElementById("myNewPassword");
    const newPwdConfirm = document.getElementById("myNewPasswordConfirm");
    const warning = document.getElementById("myPasswordWarning");
    const btnOpen = document.getElementById("btnChangeMyPassword");
    const btnConfirm = document.getElementById("btnConfirmMyPassword");

    if (!dialog || !btnOpen || !btnConfirm) return;

    btnOpen.addEventListener("click", () => {
        closeMenu();
        newPwd.value = "";
        newPwdConfirm.value = "";
        warning.style.display = "none";
        dialog.showModal();
    });

    btnConfirm.addEventListener("click", async () => {
        const pwd = newPwd.value;
        const confirm = newPwdConfirm.value;

        if (!pwd || pwd.length < 4) {
            warning.textContent = "Пароль должен быть не короче 4 символов.";
            warning.style.display = "block";
            return;
        }
        if (pwd !== confirm) {
            warning.textContent = "Пароли не совпадают.";
            warning.style.display = "block";
            return;
        }

        try {
            const response = await Auth.authFetch(`${Auth.API_BASE}/api/users/change-password`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ username: Auth.getCurrentLogin(), new_password: pwd })
            });
            if (response.ok) {
                alert("Пароль изменён");
                dialog.close();
            } else {
                const err = await response.json();
                warning.textContent = err.detail || "Ошибка";
                warning.style.display = "block";
            }
        } catch (e) {
            if (e.message !== "Unauthorized") {
                warning.textContent = "Ошибка соединения";
                warning.style.display = "block";
            }
        }
    });
}
