type AnalyticsParams = Record<string, string | number | boolean>;

declare global {
  interface Window {
    gtag?: (...args: unknown[]) => void;
  }
}

const cleanParams = (params: Record<string, unknown>) =>
  Object.fromEntries(
    Object.entries(params).filter(([, value]) => value !== undefined && value !== null),
  ) as AnalyticsParams;

const getHostname = (url: string) => {
  try {
    return new URL(url).hostname;
  } catch {
    return "";
  }
};

export const trackEvent = (name: string, params: Record<string, unknown> = {}) => {
  if (typeof window === "undefined" || typeof window.gtag !== "function") {
    return;
  }
  window.gtag("event", name, cleanParams(params));
};

export const trackNavClick = (label: string, destination: string, page: string) => {
  trackEvent("icemap_nav_click", { label, destination, page });
};

export const trackFilterChange = (
  filter: string,
  value: string | number,
  page: string,
  extra?: Record<string, unknown>,
) => {
  trackEvent("icemap_filter_change", { filter, value, page, ...extra });
};

export const trackLanguageChange = (language: string, page: string) => {
  trackEvent("icemap_language_change", { language, page });
};

export const trackOutboundClick = (
  label: string,
  url: string,
  page: string,
  context?: string,
) => {
  trackEvent("icemap_outbound_click", {
    label,
    link_url: url,
    link_domain: getHostname(url),
    page,
    context,
  });
};

export const trackLoadMore = (
  mode: "auto" | "manual",
  visibleCount: number,
  totalCount: number,
  page: string,
) => {
  trackEvent("icemap_load_more", {
    mode,
    visible_count: visibleCount,
    total_count: totalCount,
    page,
  });
};
