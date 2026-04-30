(() => {
  const MAX_BYTES = 750000;
  const form = document.querySelector("[data-profile-form]");
  const input = document.querySelector("[data-profile-photo-input]");
  const hidden = document.querySelector("[data-profile-photo-data]");
  const status = document.querySelector("[data-profile-photo-status]");
  const preview = document.querySelector("[data-profile-photo-preview] .profileUploadThumb");

  if (!form || !input || !hidden || !status) return;

  const formatBytes = (bytes) => {
    if (!Number.isFinite(bytes)) return "";
    return bytes >= 1000000 ? `${(bytes / 1000000).toFixed(2)} MB` : `${Math.ceil(bytes / 1000)} KB`;
  };

  const setStatus = (message, state = "neutral") => {
    status.textContent = message;
    status.dataset.state = state;
  };

  const dataUrlBytes = (dataUrl) => {
    const base64 = String(dataUrl || "").split(",", 2)[1] || "";
    return Math.ceil((base64.length * 3) / 4);
  };

  const renderPreview = (dataUrl) => {
    if (!preview || !dataUrl) return;
    preview.innerHTML = "";
    const img = document.createElement("img");
    img.src = dataUrl;
    img.alt = "";
    img.setAttribute("aria-hidden", "true");
    preview.appendChild(img);
  };

  const loadImage = (file) =>
    new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error("Could not read image."));
      reader.onload = () => {
        const img = new Image();
        img.onerror = () => reject(new Error("Could not process image."));
        img.onload = () => resolve(img);
        img.src = reader.result;
      };
      reader.readAsDataURL(file);
    });

  const canvasToDataUrl = (canvas, quality) => canvas.toDataURL("image/jpeg", quality);

  const compressImage = async (file) => {
    const img = await loadImage(file);
    let maxSide = 1024;
    let quality = 0.88;

    for (let attempt = 0; attempt < 9; attempt += 1) {
      const scale = Math.min(1, maxSide / Math.max(img.width, img.height));
      const width = Math.max(1, Math.round(img.width * scale));
      const height = Math.max(1, Math.round(img.height * scale));
      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext("2d", { alpha: false });
      ctx.fillStyle = "#07101c";
      ctx.fillRect(0, 0, width, height);
      ctx.drawImage(img, 0, 0, width, height);
      const dataUrl = canvasToDataUrl(canvas, quality);
      if (dataUrlBytes(dataUrl) <= MAX_BYTES) return dataUrl;
      quality = Math.max(0.58, quality - 0.08);
      maxSide = Math.max(620, Math.floor(maxSide * 0.86));
    }
    throw new Error("Profile photo must be under 750 KB. Try compressing or choosing a smaller image.");
  };

  input.addEventListener("change", async () => {
    hidden.value = "";
    const file = input.files && input.files[0];
    if (!file) {
      setStatus("Recommended: square image, under 750 KB.");
      return;
    }
    if (!/^image\/(png|jpe?g|webp|gif)$/i.test(file.type || "")) {
      input.value = "";
      setStatus("Choose a PNG, JPG, WEBP, or GIF image.", "error");
      return;
    }

    setStatus(`Selected ${file.name} (${formatBytes(file.size)}). Preparing preview...`);
    try {
      const dataUrl = file.size > MAX_BYTES ? await compressImage(file) : await compressImage(file);
      hidden.value = dataUrl;
      renderPreview(dataUrl);
      const finalSize = dataUrlBytes(dataUrl);
      setStatus(
        finalSize <= MAX_BYTES
          ? `Ready to save: ${file.name} compressed to ${formatBytes(finalSize)}.`
          : "Profile photo must be under 750 KB. Try compressing or choosing a smaller image.",
        finalSize <= MAX_BYTES ? "success" : "error",
      );
    } catch (error) {
      input.value = "";
      hidden.value = "";
      setStatus(error.message || "Profile photo must be under 750 KB.", "error");
    }
  });

  form.addEventListener("submit", (event) => {
    const file = input.files && input.files[0];
    if (file && !hidden.value) {
      event.preventDefault();
      setStatus("Profile photo must be under 750 KB. Try compressing or choosing a smaller image.", "error");
    }
  });
})();
