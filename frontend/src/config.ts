const basePath = import.meta.env.BASE_URL ?? "/";
const normalizedBase =
  basePath === "/" ? "" : basePath.replace(/\/$/, "");

export const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ??
  `${window.location.origin}${normalizedBase}`;
