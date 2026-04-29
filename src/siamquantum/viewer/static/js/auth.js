import {
  getCurrentUser,
  getSession,
  signInWithGoogle,
  signInWithPassword,
  signOut,
  signUpWithPassword,
  supabase,
  supabaseEnabled,
} from "/static/js/supabase-client.js";

let authConfigPromise = null;

async function getAuthConfig() {
  if (!authConfigPromise) {
    authConfigPromise = fetch("/api/auth/config")
      .then((response) => response.json())
      .then((payload) => payload?.data || {});
  }
  return authConfigPromise;
}

function fallbackDisplayName(user, profile) {
  return (
    profile?.display_name ||
    user?.user_metadata?.full_name ||
    user?.user_metadata?.name ||
    user?.email ||
    "Profile"
  );
}

function fallbackAvatar(user, profile) {
  return (
    profile?.avatar_url ||
    user?.user_metadata?.avatar_url ||
    user?.user_metadata?.picture ||
    ""
  );
}

function initialsFromName(name) {
  const clean = String(name || "").trim();
  if (!clean) return "PR";
  const parts = clean.split(/\s+/).slice(0, 2);
  return parts.map((part) => part[0]?.toUpperCase() || "").join("") || "PR";
}

export async function apiFetch(path, options = {}) {
  const config = await getAuthConfig();
  if (config?.local_mode) {
    const headers = new Headers(options.headers || {});
    if (!headers.has("Content-Type") && options.body && !(options.body instanceof FormData)) {
      headers.set("Content-Type", "application/json");
    }
    let response;
    try {
      response = await fetch(path, {
        ...options,
        headers,
        credentials: "same-origin",
      });
    } catch (_error) {
      throw new Error("Network error while contacting the server.");
    }
    let payload = null;
    try {
      payload = await response.json();
    } catch (_error) {
      throw new Error("Server returned an invalid response.");
    }
    if (!response.ok || !payload.ok) {
      throw new Error(payload?.error?.message || "Request failed");
    }
    return payload.data;
  }

  const session = await getSession();
  const headers = new Headers(options.headers || {});
  if (!headers.has("Content-Type") && options.body && !(options.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }
  if (session?.access_token) {
    headers.set("Authorization", `Bearer ${session.access_token}`);
  }
  let response;
  try {
    response = await fetch(path, { ...options, headers });
  } catch (_error) {
    throw new Error("Network error while contacting the server.");
  }
  let payload = null;
  try {
    payload = await response.json();
  } catch (_error) {
    throw new Error("Server returned an invalid response.");
  }
  if (!response.ok || !payload.ok) {
    throw new Error(payload?.error?.message || "Request failed");
  }
  return payload.data;
}

export async function syncProfile() {
  const config = await getAuthConfig();
  if (config?.local_mode) {
    return apiFetch("/api/auth/sync-profile", { method: "POST" });
  }
  if (!supabaseEnabled) return null;
  return apiFetch("/api/auth/sync-profile", { method: "POST" });
}

export function subscribeAuthChanges(callback) {
  getAuthConfig().then((config) => {
    if (config?.local_mode) callback(null);
  });
  if (!supabase) return () => {};
  let active = true;
  getAuthConfig().then((config) => {
    if (!active || config?.local_mode || !supabase) return;
    supabase.auth.onAuthStateChange(async (_event, session) => {
      callback(session);
    });
  });
  return () => {
    active = false;
  };
}

function applyLocalAuthToLayout(state) {
  applyAuthToLayout(state);
}

export async function loadAuthState() {
  const config = await getAuthConfig();
  if (config?.local_mode) {
    try {
      const data = await apiFetch("/api/auth/me");
      const state = {
        enabled: true,
        localMode: true,
        user: data?.user || null,
        profile: data?.profile || null,
      };
      window.__sqAuth = state;
      applyLocalAuthToLayout(state);
      return state;
    } catch (_error) {
      const state = { enabled: true, localMode: true, user: null, profile: null };
      window.__sqAuth = state;
      applyLocalAuthToLayout(state);
      return state;
    }
  }
  return loadAuthStateSupabase();
}

export function applyAuthToLayout(state) {
  const user = state?.user || null;
  const profile = state?.profile || null;
  const displayName = fallbackDisplayName(user, profile);
  const avatarUrl = fallbackAvatar(user, profile);
  const profileLinks = document.querySelectorAll("[data-profile-link]");
  const nameTargets = document.querySelectorAll("[data-profile-name]");
  const avatarTargets = document.querySelectorAll("[data-profile-avatar]");
  const authTargets = document.querySelectorAll("[data-auth-state]");
  const profileTextTargets = document.querySelectorAll("[data-profile-link] .th-text, [data-profile-link] .en-text");
  const submitNav = document.querySelector('a[href="/submit-data"]');

  profileLinks.forEach((node) => {
    node.setAttribute("href", "/profile");
  });

  profileTextTargets.forEach((node) => {
    node.textContent = user ? displayName : "Profile";
  });

  if (submitNav) {
    const spans = submitNav.querySelectorAll(".th-text, .en-text");
    spans.forEach((node) => {
      node.textContent = "Submit Data";
    });
  }

  nameTargets.forEach((node) => {
    node.textContent = user ? displayName : "Profile";
  });

  avatarTargets.forEach((node) => {
    const img = node.querySelector("img");
    if (avatarUrl) {
      if (!img) {
        const avatarImg = document.createElement("img");
        avatarImg.alt = displayName;
        avatarImg.src = avatarUrl;
        avatarImg.style.width = "100%";
        avatarImg.style.height = "100%";
        avatarImg.style.objectFit = "cover";
        avatarImg.style.borderRadius = "inherit";
        node.textContent = "";
        node.appendChild(avatarImg);
      } else {
        img.src = avatarUrl;
        img.alt = displayName;
      }
    } else {
      if (img) img.remove();
      node.textContent = initialsFromName(displayName);
    }
  });

  authTargets.forEach((node) => {
    node.textContent = user ? "Signed in" : "Guest";
  });
}

export async function loadAuthStateSupabase() {
  if (!supabaseEnabled) {
    const state = { enabled: false, user: null, profile: null };
    window.__sqAuth = state;
    applyAuthToLayout(state);
    return state;
  }

  let user = null;
  try {
    user = await getCurrentUser();
  } catch (_error) {
    const state = { enabled: true, user: null, profile: null, degraded: true };
    window.__sqAuth = state;
    applyAuthToLayout(state);
    return state;
  }
  if (!user) {
    const state = { enabled: true, user: null, profile: null };
    window.__sqAuth = state;
    applyAuthToLayout(state);
    return state;
  }

  let data = null;
  try {
    data = await syncProfile();
  } catch (_error) {
    data = null;
  }
  const state = {
    enabled: true,
    user,
    profile: data?.profile || null,
  };
  window.__sqAuth = state;
  applyAuthToLayout(state);
  return state;
}

export async function loginWithPassword(email, password) {
  const config = await getAuthConfig();
  if (config?.local_mode) {
    await apiFetch("/api/auth/local/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    return loadAuthState();
  }
  await signInWithPassword(email, password);
  return loadAuthStateSupabase();
}

export async function registerWithPassword(email, password) {
  const config = await getAuthConfig();
  if (config?.local_mode) {
    await apiFetch("/api/auth/local/register", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    await apiFetch("/api/auth/local/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    });
    return loadAuthState();
  }
  await signUpWithPassword(email, password);
  return loadAuthStateSupabase();
}

export async function loginWithGoogle() {
  const config = await getAuthConfig();
  if (config?.local_mode) {
    throw new Error("Google login is not available in local fallback mode.");
  }
  const redirectTo = `${window.location.origin}/profile`;
  await signInWithGoogle(redirectTo);
}

export async function logoutUser() {
  const config = await getAuthConfig();
  if (config?.local_mode) {
    await apiFetch("/api/auth/local/logout", { method: "POST" });
    return loadAuthState();
  }
  await signOut();
  return loadAuthStateSupabase();
}
