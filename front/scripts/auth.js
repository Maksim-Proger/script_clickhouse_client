export const API_BASE = "http://10.25.86.13:8000";

let accessToken = localStorage.getItem("token") || null;
let currentLogin = localStorage.getItem("login") || "User";
let sessionExpiredCallback = () => console.warn("Session expired handler not set");

export function isAuthenticated() {
    return !!accessToken;
}

export function getCurrentLogin() {
    return currentLogin;
}

export function setSessionExpiredHandler(callback) {
    sessionExpiredCallback = callback;
}

export async function login(login, password) {
    const response = await fetch(`${API_BASE}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ login, password })
    });

    if (response.ok) {
        const data = await response.json();
        accessToken = data.access_token;
        currentLogin = login;

        localStorage.setItem("token", accessToken);
        localStorage.setItem("login", currentLogin);
        return true;
    }
    return false;
}

export async function logout() {
    if (accessToken) {
        try {
            await fetch(`${API_BASE}/logout`, {
                method: "POST",
                headers: { "Authorization": `Bearer ${accessToken}` }
            });
        } catch (_) {}
    }
    accessToken = null;
    currentLogin = "User";
    localStorage.removeItem("token");
    localStorage.removeItem("login");
}

export async function authFetch(url, options = {}) {
    const headers = options.headers || {};

    if (accessToken) {
        headers["Authorization"] = `Bearer ${accessToken}`;
    }

    const config = {
        ...options,
        headers: headers
    };

    const response = await fetch(url, config);

    if (response.status === 401) {
        await logout();
        sessionExpiredCallback();
        throw new Error("Unauthorized");
    }

    return response;
}
