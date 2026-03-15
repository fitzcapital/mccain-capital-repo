(function () {
  const config = window.mcPasskeys || null;
  if (!config) return;

  const token = document.querySelector('meta[name="csrf-token"]')?.getAttribute("content") || "";
  const statusNode = config.statusId ? document.getElementById(config.statusId) : null;
  const labelInput = config.labelInputId ? document.getElementById(config.labelInputId) : null;
  const nextInput = config.nextInputId ? document.getElementById(config.nextInputId) : null;

  const setStatus = (message, tone) => {
    if (!statusNode) return;
    statusNode.textContent = message;
    statusNode.classList.remove("metaRed", "metaBlue", "metaGreen");
    if (tone) statusNode.classList.add(tone);
  };

  const base64urlToBuffer = (value) => {
    const padding = "=".repeat((4 - (value.length % 4 || 4)) % 4);
    const base64 = (value + padding).replace(/-/g, "+").replace(/_/g, "/");
    const binary = window.atob(base64);
    return Uint8Array.from(binary, (ch) => ch.charCodeAt(0));
  };

  const bufferToBase64url = (buffer) => {
    const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer || []);
    let binary = "";
    bytes.forEach((byte) => {
      binary += String.fromCharCode(byte);
    });
    return window.btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
  };

  const normalizeCreationOptions = (options) => {
    const publicKey = { ...options };
    publicKey.challenge = base64urlToBuffer(publicKey.challenge);
    publicKey.user = { ...publicKey.user, id: base64urlToBuffer(publicKey.user.id) };
    publicKey.excludeCredentials = (publicKey.excludeCredentials || []).map((cred) => ({
      ...cred,
      id: base64urlToBuffer(cred.id),
    }));
    return publicKey;
  };

  const normalizeRequestOptions = (options) => {
    const publicKey = { ...options };
    publicKey.challenge = base64urlToBuffer(publicKey.challenge);
    publicKey.allowCredentials = (publicKey.allowCredentials || []).map((cred) => ({
      ...cred,
      id: base64urlToBuffer(cred.id),
    }));
    return publicKey;
  };

  const credentialToJSON = (credential) => {
    if (!credential) return null;
    return {
      id: credential.id,
      rawId: bufferToBase64url(credential.rawId),
      type: credential.type,
      response: credential.response
        ? {
            clientDataJSON: bufferToBase64url(credential.response.clientDataJSON),
            attestationObject: credential.response.attestationObject
              ? bufferToBase64url(credential.response.attestationObject)
              : undefined,
            authenticatorData: credential.response.authenticatorData
              ? bufferToBase64url(credential.response.authenticatorData)
              : undefined,
            signature: credential.response.signature
              ? bufferToBase64url(credential.response.signature)
              : undefined,
            userHandle: credential.response.userHandle
              ? bufferToBase64url(credential.response.userHandle)
              : undefined,
            transports:
              typeof credential.response.getTransports === "function"
                ? credential.response.getTransports()
                : undefined,
          }
        : undefined,
      clientExtensionResults:
        typeof credential.getClientExtensionResults === "function"
          ? credential.getClientExtensionResults()
          : {},
      authenticatorAttachment: credential.authenticatorAttachment || undefined,
    };
  };

  const postJSON = async (url, payload) => {
    const response = await window.fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRF-Token": token,
      },
      body: JSON.stringify(payload || {}),
      credentials: "same-origin",
    });
    const isJson = (response.headers.get("content-type") || "").includes("application/json");
    const body = isJson ? await response.json() : await response.text();
    if (!response.ok) {
      const message =
        (body && typeof body === "object" && (body.error || body.description || body.message)) ||
        (typeof body === "string" ? body : "Passkey action failed.");
      throw new Error(message);
    }
    return body;
  };

  const runRegister = async (button) => {
    if (!window.PublicKeyCredential) {
      setStatus("This browser does not support passkeys on this device.", "metaRed");
      return;
    }
    button.disabled = true;
    setStatus("Preparing secure registration options…", "metaBlue");
    try {
      const optionsPayload = await postJSON(config.registerOptionsUrl, {});
      const credential = await navigator.credentials.create({
        publicKey: normalizeCreationOptions(optionsPayload.publicKey || optionsPayload),
      });
      await postJSON(config.registerVerifyUrl, {
        label: labelInput ? labelInput.value : "",
        credential: credentialToJSON(credential),
      });
      setStatus("Passkey registered. Refreshing the device list…", "metaGreen");
      window.setTimeout(() => window.location.reload(), 700);
    } catch (error) {
      setStatus(error.message || "Passkey registration failed.", "metaRed");
      button.disabled = false;
    }
  };

  const runLogin = async (button) => {
    if (!window.PublicKeyCredential) {
      setStatus("This browser does not support passkeys on this device.", "metaRed");
      return;
    }
    button.disabled = true;
    setStatus("Waiting for your device to approve sign-in…", "metaBlue");
    try {
      const nextUrl = nextInput ? nextInput.value : "";
      const optionsPayload = await postJSON(config.authOptionsUrl, { next: nextUrl });
      const credential = await navigator.credentials.get({
        publicKey: normalizeRequestOptions(optionsPayload.publicKey || optionsPayload),
      });
      const result = await postJSON(config.authVerifyUrl, {
        next: nextUrl,
        credential: credentialToJSON(credential),
      });
      window.location.assign(result.redirect || "/dashboard");
    } catch (error) {
      setStatus(error.message || "Passkey sign-in failed.", "metaRed");
      button.disabled = false;
    }
  };

  const registerButton = document.querySelector("[data-passkey-register]");
  if (registerButton) {
    registerButton.addEventListener("click", () => runRegister(registerButton));
  }

  const loginButton = document.querySelector("[data-passkey-login]");
  if (loginButton) {
    loginButton.addEventListener("click", () => runLogin(loginButton));
  }
})();
