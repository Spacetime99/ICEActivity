const basePath = import.meta.env.BASE_URL ?? "/";
const normalizedBase =
  basePath === "/" ? "" : basePath.replace(/\/$/, "");
const isLocalDev =
  import.meta.env.DEV &&
  (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1");

export const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ??
  `${window.location.origin}${normalizedBase}`;

export const STATIC_DATA_BASE_URL =
  import.meta.env.VITE_STATIC_DATA_BASE_URL ??
  (isLocalDev
    ? `${window.location.origin}/data`
    : `${window.location.origin}${normalizedBase}/data`);
