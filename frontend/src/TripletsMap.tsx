import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
  useMap,
  Marker,
  Tooltip,
  Pane,
} from "react-leaflet";
import L, { LatLngBoundsExpression } from "leaflet";
import { STATIC_DATA_BASE_URL } from "./config";
import { STATIC_LOCATIONS, type StaticLocation } from "./staticLocations";
import { FIELD_OFFICES } from "./fieldOffices";
import { DETENTION_FACILITIES } from "./detentionFacilities";
import type { DetentionFacility } from "./detentionFacilities";
import { getResourceSections } from "./resources";
import type { FieldOffice } from "./fieldOffices";
import { DEFAULT_LANGUAGE, LANGUAGE_LABELS, TRANSLATIONS, type Language } from "./i18n";
import ChartsView from "./ChartsView";
import {
  buildTripletBlob,
  isChildMention,
  isUsStatusMention,
} from "./mentionPatterns";
import { ABOUT_CONTENT, METHODOLOGY_CONTENT, getOverlayText } from "./overlays";
import {
  trackFilterChange,
  trackLanguageChange,
  trackNavClick,
  trackOutboundClick,
} from "./analytics";
import {
  AboutIcon,
  ChartsIcon,
  FeedbackIcon,
  HeadlinesIcon,
  MapIcon,
  ProtestsIcon,
  ResourcesIcon,
  StatsIcon,
} from "./navIcons";
import PageHeader from "./PageHeader";

type Triplet = {
  story_id: string;
  title: string;
  who: string;
  what: string;
  where_text: string | null;
  lat: number;
  lon: number;
  url?: string | null;
  publishedAt: string;
  source?: string | null;
  eventTypes?: string[];
};

type TripletGroup = {
  key: string;
  lat: number;
  lon: number;
  items: Triplet[];
};

type StoryGroup = {
  storyId: string;
  title: string;
  url?: string | null;
  publishedAt?: string | null;
  items: Triplet[];
};

type TimeRangeValue = number | "all";
type ViewMode = "map" | "resources" | "charts";
type EventFilter = "all" | "protest" | "severe";

// Keep one hour under the API's 90-day upper bound to avoid validation errors on
// deployments that still enforce a strict "< 90 days" check.
const MAX_API_WINDOW_HOURS = 24 * 90 - 1;

const TIME_RANGES: Array<{
  label: { en: string; es: string };
  summary: { en: string; es: string };
  value: TimeRangeValue;
}> = [
  { label: { en: "3d", es: "3d" }, summary: { en: "in the last 3 days", es: "en los últimos 3 días" }, value: 72 },
  { label: { en: "7d", es: "7d" }, summary: { en: "in the last 7 days", es: "en los últimos 7 días" }, value: 24 * 7 },
  { label: { en: "1mo", es: "1 mes" }, summary: { en: "in the last month", es: "en el último mes" }, value: 24 * 30 },
  { label: { en: "3mo", es: "3 meses" }, summary: { en: "in the last 3 months", es: "en los últimos 3 meses" }, value: MAX_API_WINDOW_HOURS },
  { label: { en: "All", es: "Todo" }, summary: { en: "all dates", es: "todas las fechas" }, value: "all" },
];

const EVENT_FILTERS: Array<{
  labelKey: "filterAll" | "filterProtest" | "filterSevere";
  value: EventFilter;
}> = [
  { labelKey: "filterAll", value: "all" },
  { labelKey: "filterProtest", value: "protest" },
  { labelKey: "filterSevere", value: "severe" },
];

const formatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "short",
  timeStyle: "short",
});
const dayFormatter = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });

const LEGEND = [
  { label: "< 6h", color: "#ff4d4f" },
  { label: "6–24h", color: "#ff7a45" },
  { label: "1–3d", color: "#ffa940" },
  { label: "3–7d", color: "#ffc069" },
  { label: "7–30d", color: "#ffd591" },
];

const SEVERE_COLOR = "#8b0000";
const PROTEST_COLOR = "#2f9e44";
const UNREST_COLOR = "#6f2dbd";
const numberFormatter = new Intl.NumberFormat(undefined);
const FEEDBACK_URL = "https://tally.so/r/lbOAvo";
const ANALYTICS_PAGE = "map";
const SEVERE_KEYWORDS = [
  "shot",
  "shooting",
  "killed",
  "killing",
  "injured",
  "injury",
  "wounded",
  "dead",
  "fatal",
  "murder",
  "assault",
  "assaulted",
  "beaten",
  "violence",
  "wound",
];
const PROTEST_TYPES = new Set([
  "protest",
  "march",
  "rally",
  "demonstration",
  "strike",
  "walkout",
  "picket",
  "sit_in",
  "vigil",
  "boycott",
  "blockade",
  "occupation",
  "riot",
  "civil_unrest",
  "uprising",
  "revolution",
]);
const UNREST_TYPES = new Set([
  "riot",
  "civil_unrest",
  "blockade",
  "uprising",
  "revolution",
]);

function matchesEventFilter(triplet: Triplet, filter: EventFilter): boolean {
  if (filter === "all") {
    return true;
  }
  const types = triplet.eventTypes ?? [];
  const hasProtest = types.some((type) => PROTEST_TYPES.has(type));
  if (filter === "protest") {
    return hasProtest;
  }
  if (filter === "severe") {
    return isSevereTriplet(triplet);
  }
  return true;
}

const GENERAL_LOCATION_TOLERANCE = 0.1;
const GENERAL_COORDINATES = { lat: 39.7837304, lon: -100.445882 };
const DC_COORDINATES = { lat: 38.9072, lon: -77.0369 };
const DC_LOCATION_TOLERANCE = 0.15;

const FACILITY_LOCATIONS = STATIC_LOCATIONS.filter((loc) => loc.type === "facility");
const CHILD_CAMP_LOCATIONS = STATIC_LOCATIONS.filter((loc) => loc.type === "child_camp");
const FACILITY_COUNT = FACILITY_LOCATIONS.length;
const CHILD_CAMP_COUNT = CHILD_CAMP_LOCATIONS.length;

const SOURCE_LOGO_EXTS = [".png", ".svg", ".ico"];
const SOURCE_LOGO_BASE = import.meta.env.BASE_URL ?? "/";
const NAV_BASE_URL = import.meta.env.BASE_URL ?? "/";
const HEADLINES_URL = `${NAV_BASE_URL}headlines.html`;
const PROTESTS_URL = `${NAV_BASE_URL}protests.html`;
const STATS_URL = `${NAV_BASE_URL}stats.html`;
const CHARTS_URL = `${NAV_BASE_URL}charts.html`;
const DEATHS_URL = `${NAV_BASE_URL}deaths.html`;
const DEFAULT_TOPIC = "Immigration";
const DEFAULT_IMPACT = "Enforcement";

function groupTriplets(data: Triplet[]): TripletGroup[] {
  const groups = new Map<string, TripletGroup>();
  data.forEach((triplet) => {
    const key = `${triplet.lat.toFixed(3)}|${triplet.lon.toFixed(3)}`;
    const existing = groups.get(key);
    if (existing) {
      existing.items.push(triplet);
    } else {
      groups.set(key, {
        key,
        lat: triplet.lat,
        lon: triplet.lon,
        items: [triplet],
      });
    }
  });
  return Array.from(groups.values()).sort(
    (a, b) => b.items.length - a.items.length,
  );
}

function groupByStory(data: Triplet[]): StoryGroup[] {
  const groups = new Map<string, StoryGroup>();
  data.forEach((triplet) => {
    const key = triplet.story_id || triplet.url || triplet.title;
    const existing = groups.get(key);
    if (existing) {
      existing.items.push(triplet);
      if (!existing.title && triplet.title) {
        existing.title = triplet.title;
      }
      if (!existing.url && triplet.url) {
        existing.url = triplet.url;
      }
      if (
        triplet.publishedAt &&
        (!existing.publishedAt ||
          new Date(triplet.publishedAt).getTime() >
            new Date(existing.publishedAt).getTime())
      ) {
        existing.publishedAt = triplet.publishedAt;
      }
    } else {
      groups.set(key, {
        storyId: key,
        title: triplet.title,
        url: triplet.url,
        publishedAt: triplet.publishedAt,
        items: [triplet],
      });
    }
  });
  return Array.from(groups.values()).sort((a, b) => {
    const aTime = a.publishedAt ? new Date(a.publishedAt).getTime() : 0;
    const bTime = b.publishedAt ? new Date(b.publishedAt).getTime() : 0;
    return bTime - aTime;
  });
}

function getMarkerColor(publishedAt: string): string {
  const hoursAgo =
    (Date.now() - new Date(publishedAt).getTime()) / (1000 * 60 * 60);
  if (hoursAgo <= 6) {
    return "#ff4d4f";
  }
  if (hoursAgo <= 24) {
    return "#ff7a45";
  }
  if (hoursAgo <= 72) {
    return "#ffa940";
  }
  if (hoursAgo <= 24 * 7) {
    return "#ffc069";
  }
  return "#ffd591";
}

function isSevereTriplet(triplet: Triplet): boolean {
  const blob = [triplet.what, triplet.title, triplet.who]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  if (!blob) {
    return false;
  }
  return SEVERE_KEYWORDS.some((keyword) => blob.includes(keyword));
}

function isChildTriplet(triplet: Triplet): boolean {
  const blob = buildTripletBlob(triplet);
  if (!blob) {
    return false;
  }
  return isChildMention(blob);
}

function isUsNationalEnforcementTriplet(triplet: Triplet): boolean {
  const blob = buildTripletBlob(triplet);
  if (!blob) {
    return false;
  }
  return isUsStatusMention(blob);
}

function isUnrestTriplet(triplet: Triplet): boolean {
  const types = triplet.eventTypes ?? [];
  return types.some((type) => UNREST_TYPES.has(type));
}

function isProtestTriplet(triplet: Triplet): boolean {
  const types = triplet.eventTypes ?? [];
  return types.some((type) => PROTEST_TYPES.has(type) && !UNREST_TYPES.has(type));
}

function formatPopulationLabel(value?: string | null): string {
  if (!value) {
    return "";
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return "";
  }
  return numberFormatter.format(parsed);
}

const SOURCE_STOP_WORDS = new Set([
  "the",
  "and",
  "of",
  "in",
  "at",
  "for",
  "on",
  "by",
  "to",
  "from",
]);

const formatSourceLabel = (source?: string | null): string | null => {
  if (!source) {
    return null;
  }
  let label = source.trim();
  if (!label) {
    return null;
  }
  const looksLikeHostname =
    !label.includes(" ") && label.includes(".") && !label.includes(":");
  if (looksLikeHostname) {
    label = label.replace(/^www\./, "");
    return label;
  }
  if (label.includes("://")) {
    try {
      const parsed = new URL(label);
      label = parsed.hostname;
    } catch {
      return null;
    }
  } else if (label.includes(":")) {
    label = label.split(":").pop() ?? label;
  }
  label = label.replace(/^(html|rss|rssapp|newsapi)[_-]+/i, "");
  label = label.replace(/^www\./, "");
  label = label.replace(/[_-]+/g, " ");
  const words = label.split(/\s+/).filter(Boolean);
  if (!words.length) {
    return null;
  }
  return words
    .map((word, index) => {
      const cleaned = word.replace(/[^\w]/g, "");
      if (!cleaned) {
        return word;
      }
      const lower = cleaned.toLowerCase();
      if (index > 0 && SOURCE_STOP_WORDS.has(lower)) {
        return lower;
      }
      if (/^[a-z]+$/.test(cleaned) && cleaned.length <= 3) {
        return cleaned.toUpperCase();
      }
      return cleaned[0].toUpperCase() + cleaned.slice(1).toLowerCase();
    })
    .join(" ");
};

const getLocationLabel = (items: Triplet[]): string | null => {
  for (const item of items) {
    const label = item.where_text?.trim();
    if (!label) {
      continue;
    }
    const lower = label.toLowerCase();
    if (
      lower.includes("no specific location") ||
      lower.includes("not specified") ||
      lower.includes("not provided") ||
      lower.includes("unspecified")
    ) {
      continue;
    }
    return label;
  }
  return null;
};

const getTopicLabel = (items: Triplet[]): string => {
  const blob = items
    .map((item) => `${item.title} ${item.what}`)
    .join(" ")
    .toLowerCase();
  if (/(court|judge|lawsuit|appeal|ruling|injunction)/.test(blob)) {
    return "Courts";
  }
  if (/(politic|election|campaign|congress|senate|house|governor|mayor|president)/.test(blob)) {
    return "Politics";
  }
  return DEFAULT_TOPIC;
};

const getImpactLabel = (items: Triplet[]): string => {
  const blob = items
    .map((item) => `${item.title} ${item.what}`)
    .join(" ")
    .toLowerCase();
  if (/(raid|raided)/.test(blob)) {
    return "Raid";
  }
  if (/(arrest|arrested|detain|detained|apprehend|custody)/.test(blob)) {
    return "Arrests";
  }
  if (/(deport|deported|deportation|removed|removal)/.test(blob)) {
    return "Deportations";
  }
  if (/(lawsuit|suit|complaint|litigation)/.test(blob)) {
    return "Lawsuit";
  }
  if (/(policy|ban|rule|order|directive|regulation)/.test(blob)) {
    return "Policy";
  }
  return DEFAULT_IMPACT;
};

const slugifySource = (source?: string | null): string | null => {
  if (!source) {
    return null;
  }
  const cleaned = source.trim().toLowerCase();
  if (!cleaned) {
    return null;
  }
  return cleaned.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || null;
};

const SourceLogo: React.FC<{
  primaryKey?: string | null;
  fallbackKey?: string | null;
  alt: string;
}> = ({ primaryKey, fallbackKey, alt }) => {
  const keys = [primaryKey, fallbackKey].filter(Boolean) as string[];
  const candidates = keys.flatMap((key) => {
    const slug = slugifySource(key);
    if (!slug) {
      return [];
    }
    return SOURCE_LOGO_EXTS.map(
      (ext) => `${SOURCE_LOGO_BASE}source-logos/${slug}${ext}`,
    );
  });
  const [candidateIndex, setCandidateIndex] = useState(0);
  const src = candidates[candidateIndex];
  const fallbackLetter = alt.trim().charAt(0).toUpperCase() || "?";
  const checkTransparency = (event: React.SyntheticEvent<HTMLImageElement>) => {
    const img = event.currentTarget;
    if (!img.naturalWidth || !img.naturalHeight) {
      setCandidateIndex((prev) => prev + 1);
      return;
    }
    const canvas = document.createElement("canvas");
    canvas.width = img.naturalWidth;
    canvas.height = img.naturalHeight;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
      return;
    }
    ctx.drawImage(img, 0, 0);
    try {
      const data = ctx.getImageData(0, 0, canvas.width, canvas.height).data;
      let transparent = 0;
      for (let i = 3; i < data.length; i += 4) {
        if (data[i] < 16) {
          transparent += 1;
        }
      }
      if (transparent / (data.length / 4) > 0.98) {
        setCandidateIndex((prev) => prev + 1);
      }
    } catch {
      // Ignore cross-origin or canvas errors; keep the loaded image.
    }
  };
  if (!src) {
    return (
      <span className="source-logo fallback" aria-hidden="true">
        {fallbackLetter}
      </span>
    );
  }
  return (
    <img
      src={src}
      alt={alt}
      className="source-logo"
      onError={() => setCandidateIndex((prev) => prev + 1)}
      onLoad={checkTransparency}
    />
  );
};

const MapBounds: React.FC = () => {
  const map = useMap();
  const bounds = useMemo(
    () =>
      L.latLngBounds(
        [24.0, -125.0],
        [50.0, -66.5],
      ).pad(0.12) satisfies LatLngBoundsExpression,
    [],
  );
  useEffect(() => {
    map.setMaxBounds(bounds);
    map.setMinZoom(3);
    map.options.maxBoundsViscosity = 0.6;
  }, [map, bounds]);
  return null;
};

const getInitialLanguage = (): Language => {
  if (typeof window === "undefined") {
    return DEFAULT_LANGUAGE;
  }
  const stored = window.localStorage.getItem("icemap_lang");
  if (stored === "en" || stored === "es") {
    return stored;
  }
  const browser = navigator.language?.toLowerCase() ?? "";
  return browser.startsWith("es") ? "es" : DEFAULT_LANGUAGE;
};

const TripletsMap = () => {
  const [language, setLanguage] = useState<Language>(getInitialLanguage);
  const [sinceHours, setSinceHours] = useState<TimeRangeValue>("all");
  const [eventFilter, setEventFilter] = useState<EventFilter>("all");
  const [triplets, setTriplets] = useState<Triplet[]>([]);
  const [generalTriplets, setGeneralTriplets] = useState<Triplet[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [showFacilities, setShowFacilities] = useState(true);
  const [showChildCamps, setShowChildCamps] = useState(true);
  const [showFieldOffices, setShowFieldOffices] = useState(false);
  const [showDetentionFacilities, setShowDetentionFacilities] = useState(false);
  const [viewport, setViewport] = useState<"desktop" | "tablet" | "mobile">("desktop");
  const [viewMode, setViewMode] = useState<ViewMode>("map");
  const [selectedGroupKey, setSelectedGroupKey] = useState<string | null>(null);
  const [selectedFieldOffice, setSelectedFieldOffice] = useState<FieldOffice | null>(null);
  const [selectedDetentionFacility, setSelectedDetentionFacility] =
    useState<DetentionFacility | null>(null);
  const [selectedStaticLocation, setSelectedStaticLocation] =
    useState<StaticLocation | null>(null);
  const [showAbout, setShowAbout] = useState(false);
  const [showMethodology, setShowMethodology] = useState(false);
  const mapRef = useRef<L.Map | null>(null);
  const t = TRANSLATIONS[language];
  const trackNav = (label: string, destination: string) =>
    trackNavClick(label, destination, ANALYTICS_PAGE);
  const formatPublishedAt = (value?: string | null) => {
    if (!value) {
      return t.unknownDate;
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return t.unknownDate;
    }
    return formatter.format(parsed);
  };
  const formatPublishedDay = (value?: string | null) => {
    if (!value) {
      return t.unknownDate;
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return t.unknownDate;
    }
    return dayFormatter.format(parsed);
  };
  const formatUpdatedAt = (value?: string | null) => {
    if (!value) {
      return null;
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return formatter.format(parsed);
  };
  const getStaticLocationTypeLabel = (location: StaticLocation) =>
    location.type === "facility" ? t.facilitiesLabel : t.unaccompaniedChildrenSite;
  const getSourceLabels = (items: Triplet[]) => {
    const labels: Array<{
      label: string;
      logoKey: string | null;
      fallbackKey: string | null;
    }> = [];
    const seen = new Set<string>();
    items.forEach((item) => {
      const source = item.source?.trim() ?? "";
      const url = item.url?.trim() ?? "";
      let hostname = "";
      if (url) {
        try {
          hostname = new URL(url).hostname.replace(/^www\./, "");
        } catch {
          hostname = "";
        }
      }
      const key = (hostname || source).toLowerCase();
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      const label = formatSourceLabel(hostname || source) ?? t.unknownSource;
      labels.push({
        label,
        logoKey: hostname || null,
        fallbackKey: source || null,
      });
    });
    return labels.length
      ? labels
      : [{ label: t.unknownSource, logoKey: null, fallbackKey: null }];
  };

  const getItemSourceLabel = (item: Triplet): string => {
    const url = item.url?.trim() ?? "";
    let hostname = "";
    if (url) {
      try {
        hostname = new URL(url).hostname.replace(/^www\./, "");
      } catch {
        hostname = "";
      }
    }
    return formatSourceLabel(hostname || item.source) ?? t.unknownSource;
  };

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("icemap_lang", language);
    }
  }, [language]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const handleHash = () => {
      const hash = window.location.hash;
      if (hash === "#about") {
        setShowAbout(true);
        setShowMethodology(false);
        setViewMode("map");
      } else if (hash === "#methodology") {
        setShowMethodology(true);
        setShowAbout(false);
        setViewMode("map");
      } else if (hash === "#resources") {
        setViewMode("resources");
        setShowAbout(false);
        setShowMethodology(false);
      } else if (hash === "#charts") {
        setViewMode("charts");
        setShowAbout(false);
        setShowMethodology(false);
      } else {
        setViewMode("map");
        setShowAbout(false);
        setShowMethodology(false);
      }
    };
    handleHash();
    window.addEventListener("hashchange", handleHash);
    return () => window.removeEventListener("hashchange", handleHash);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    async function fetchMeta() {
      try {
        const response = await fetch(`${STATIC_DATA_BASE_URL}/triplets_meta.json`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`Static metadata request failed (${response.status})`);
        }
        const payload = (await response.json()) as { updatedAt?: string };
        if (payload.updatedAt) {
          setLastUpdated(payload.updatedAt);
        }
      } catch (err) {
        if ((err as DOMException).name !== "AbortError") {
          setLastUpdated(null);
        }
      }
    }
    fetchMeta();
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    async function fetchTriplets() {
      setLoading(true);
      setError(null);
      try {
        const rangeKey =
          sinceHours === "all"
            ? "all"
            : sinceHours <= 72
              ? "3d"
              : sinceHours <= 24 * 7
                ? "7d"
                : sinceHours <= 24 * 30
                  ? "1mo"
                  : "3mo";
        const response = await fetch(`${STATIC_DATA_BASE_URL}/triplets_${rangeKey}.json`, {
          signal: controller.signal,
        });
        const payload = await response.text();
        if (!response.ok) {
          throw new Error(`Static data request failed (${response.status})`);
        }
        const json = JSON.parse(payload) as Triplet[];
        const partitioned = json.reduce(
          (acc, item) => {
            const whereLower = item.where_text?.toLowerCase() ?? "";
            const hasCoords =
              Number.isFinite(item.lat) && Number.isFinite(item.lon);
            const matchesGeneralText =
              whereLower.includes("united states") ||
              whereLower.includes("washington, dc") ||
              whereLower.includes("washington dc") ||
              whereLower.includes("washington d.c.");
            const nearGeneralCenter =
              Math.abs(item.lat - GENERAL_COORDINATES.lat) <
                GENERAL_LOCATION_TOLERANCE &&
              Math.abs(item.lon - GENERAL_COORDINATES.lon) <
                GENERAL_LOCATION_TOLERANCE;
            const nearDc =
              Math.abs(item.lat - DC_COORDINATES.lat) < DC_LOCATION_TOLERANCE &&
              Math.abs(item.lon - DC_COORDINATES.lon) < DC_LOCATION_TOLERANCE;
            const isGeneral =
              !hasCoords || matchesGeneralText || nearGeneralCenter || nearDc;
            if (isGeneral) {
              acc.general.push(item);
            } else {
              acc.precise.push(item);
            }
            return acc;
          },
          { precise: [] as Triplet[], general: [] as Triplet[] },
        );
        setTriplets(partitioned.precise);
        setGeneralTriplets(partitioned.general);
      } catch (err) {
        if ((err as DOMException).name !== "AbortError") {
          setError((err as Error).message);
        }
      } finally {
        setLoading(false);
      }
    }
    fetchTriplets();
    return () => controller.abort();
  }, [sinceHours]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const updateViewport = () => {
      const width = window.innerWidth;
      if (width < 640) {
        setViewport("mobile");
      } else if (width < 1024) {
        setViewport("tablet");
      } else {
        setViewport("desktop");
      }
    };
    updateViewport();
    window.addEventListener("resize", updateViewport);
    return () => window.removeEventListener("resize", updateViewport);
  }, []);

  const filteredTriplets = useMemo(
    () => triplets.filter((triplet) => matchesEventFilter(triplet, eventFilter)),
    [triplets, eventFilter],
  );
  const filteredGeneralTriplets = useMemo(
    () => generalTriplets.filter((triplet) => matchesEventFilter(triplet, eventFilter)),
    [generalTriplets, eventFilter],
  );
  const groups = useMemo(() => groupTriplets(filteredTriplets), [filteredTriplets]);
  const selectedGroup = useMemo(() => {
    if (!selectedGroupKey) {
      return null;
    }
    return groups.find((group) => group.key === selectedGroupKey) ?? null;
  }, [groups, selectedGroupKey]);
  const sortedTriplets = useMemo(
    () =>
      filteredTriplets
        .slice()
        .sort(
          (a, b) => new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime(),
        ),
    [filteredTriplets],
  );
  const groupedTriplets = useMemo(() => groupByStory(sortedTriplets), [sortedTriplets]);
  const groupedGeneralTriplets = useMemo(
    () => groupByStory(filteredGeneralTriplets),
    [filteredGeneralTriplets],
  );
  const groupedGeneralTripletsByDay = useMemo(() => {
    const dayGroups: Array<{ label: string; items: StoryGroup[] }> = [];
    let currentLabel = "";
    groupedGeneralTriplets.forEach((item) => {
      const label = formatPublishedDay(item.publishedAt);
      if (label !== currentLabel) {
        currentLabel = label;
        dayGroups.push({ label, items: [] });
      }
      dayGroups[dayGroups.length - 1].items.push(item);
    });
    return dayGroups;
  }, [groupedGeneralTriplets, language]);
  const totalEvents = filteredTriplets.length + filteredGeneralTriplets.length;
  const activeRangeSummary =
    TIME_RANGES.find((range) => range.value === sinceHours)?.summary[language] ??
    TIME_RANGES[TIME_RANGES.length - 1].summary[language];
  const activeFilterSummary =
    eventFilter === "protest"
      ? t.filterSummaryProtest
      : eventFilter === "severe"
        ? t.filterSummarySevere
        : t.filterSummaryAll;
  const countSummary =
    `${totalEvents} ${t.eventsLabel}, ` +
    `${filteredTriplets.length} ${t.mappedLabel}, ` +
    `${filteredGeneralTriplets.length} ${t.generalCountLabel}`;
  const headerSummary = `${countSummary} — ${activeFilterSummary} — ${activeRangeSummary}`;
  const headerTitle =
    viewMode === "resources"
      ? t.resourcesTab
      : viewMode === "charts"
        ? t.chartsTab
        : t.appLongName;

  const facilityIcon = useMemo(
    () =>
      new L.DivIcon({
        className: "custom-marker facility-marker",
        html: "<div>▲</div>",
        iconSize: [28, 28],
        iconAnchor: [14, 14],
      }),
    [],
  );
  const detentionFacilities = useMemo(
    () =>
      DETENTION_FACILITIES.filter(
        (facility): facility is DetentionFacility & { latitude: number; longitude: number } =>
          facility.latitude !== null && facility.longitude !== null,
      ),
    [],
  );
  const fieldOfficeIcon = useMemo(
    () =>
      new L.DivIcon({
        className: "custom-marker field-office-marker",
        html: "<div>▲</div>",
        iconSize: [18, 18],
        iconAnchor: [9, 9],
      }),
    [],
  );
  const detentionCampIcon = useMemo(
    () =>
      new L.DivIcon({
        className: "custom-marker camp-marker",
        html: '<div class="detention-marker-symbol">☠</div>',
        iconSize: [60, 60],
        iconAnchor: [30, 30],
      }),
    [],
  );
  const detentionIconCache = useMemo(() => new Map<string, L.DivIcon>(), []);
  const childCampIcon = useMemo(
    () =>
      new L.DivIcon({
        className: "custom-marker child-camp-marker",
        html: "<div>🧒</div>",
        iconSize: [30, 30],
        iconAnchor: [15, 15],
      }),
    [],
  );
  const popupProps = {
    autoPan: true,
    autoPanPadding: [32, 32] as L.PointExpression,
    autoPanPaddingTopLeft: [80, 80] as L.PointExpression,
    autoPanPaddingBottomRight: [40, 40] as L.PointExpression,
    maxWidth: 320,
    maxHeight: 240,
  };

  const isMobile = viewport === "mobile";
  const isTablet = viewport === "tablet";
  const getDetentionIcon = (label: string) => {
    if (!label) {
      return detentionCampIcon;
    }
    const cached = detentionIconCache.get(label);
    if (cached) {
      return cached;
    }
    const icon = new L.DivIcon({
      className: "custom-marker camp-marker detention-marker",
      html: `<div class="detention-marker-wrap"><div class="detention-marker-symbol">☠</div><div class="detention-marker-label">${label}</div></div>`,
      iconSize: [80, 72],
      iconAnchor: [40, 36],
    });
    detentionIconCache.set(label, icon);
    return icon;
  };
  const focusMapOnMarker = (lat: number, lon: number) => {
    if (isMobile) {
      return;
    }
    const map = mapRef.current;
    if (!map) {
      return;
    }
    const targetZoom = Math.min(map.getZoom() + 1, 8);
    map.flyTo([lat, lon], targetZoom, { duration: 0.6 });
    map.panBy([0, -140], { animate: true });
  };
  const clearHash = () => {
    if (typeof window === "undefined") {
      return;
    }
    const base = window.location.pathname + window.location.search;
    window.history.replaceState(null, "", base);
  };
  const openMapView = () => {
    setViewMode("map");
    setShowAbout(false);
    setShowMethodology(false);
    clearHash();
    trackNav("map", "map");
  };
  const openResources = () => {
    setViewMode("resources");
    setShowAbout(false);
    setShowMethodology(false);
    if (typeof window !== "undefined") {
      window.history.replaceState(null, "", "#resources");
    }
    trackNav("resources", "#resources");
  };
  const openAbout = () => {
    setShowAbout(true);
    setShowMethodology(false);
    if (typeof window !== "undefined") {
      window.history.replaceState(null, "", "#about");
    }
    trackNav("about", "#about");
  };
  const openMethodology = () => {
    setShowMethodology(true);
    setShowAbout(false);
    if (typeof window !== "undefined") {
      window.history.replaceState(null, "", "#methodology");
    }
    trackNav("methodology", "#methodology");
  };
  const closeOverlays = () => {
    setShowAbout(false);
    setShowMethodology(false);
    clearHash();
  };
  const openFeedback = () => {
    if (typeof window === "undefined") {
      return;
    }
    trackOutboundClick("feedback", FEEDBACK_URL, ANALYTICS_PAGE, "nav");
    window.open(FEEDBACK_URL, "_blank", "noopener");
  };
  const handleLanguageChange = (value: Language) => {
    setLanguage(value);
    trackLanguageChange(value, ANALYTICS_PAGE);
  };
  const handleRangeChange = (value: TimeRangeValue) => {
    setSinceHours(value);
    trackFilterChange("time_range", value === "all" ? "all" : value, ANALYTICS_PAGE);
  };
  const handleEventFilterChange = (value: EventFilter) => {
    setEventFilter(value);
    trackFilterChange("event_filter", value, ANALYTICS_PAGE);
  };
  const trackLayerToggle = (layer: string, enabled: boolean) => {
    trackFilterChange("layer_toggle", layer, ANALYTICS_PAGE, { enabled });
  };
  const handleOutboundClick =
    (label: string, url?: string | null, context?: string) => () => {
      if (!url) {
        return;
      }
      trackOutboundClick(label, url, ANALYTICS_PAGE, context);
    };
  const selectedItems = useMemo(() => {
    if (!selectedGroup) {
      return [] as Triplet[];
    }
    return selectedGroup.items
      .slice()
      .sort(
        (a, b) => new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime(),
      );
  }, [selectedGroup]);
  const selectedLocationLabel = selectedItems[0]?.where_text
    ? selectedItems[0]?.where_text
    : selectedGroup
      ? `${selectedGroup.lat.toFixed(2)}, ${selectedGroup.lon.toFixed(2)}`
      : "";
  const clearSelection = () => {
    setSelectedGroupKey(null);
    setSelectedFieldOffice(null);
    setSelectedDetentionFacility(null);
    setSelectedStaticLocation(null);
  };

  useEffect(() => {
    if (!isMobile || viewMode !== "map") {
      setSelectedGroupKey(null);
      setSelectedFieldOffice(null);
      setSelectedDetentionFacility(null);
      setSelectedStaticLocation(null);
    }
  }, [isMobile, viewMode]);

  useEffect(() => {
    if (selectedGroupKey && !selectedGroup) {
      setSelectedGroupKey(null);
    }
  }, [selectedGroupKey, selectedGroup]);
  const brandCard = (
    <div className="brand-card">
      <img
        className="brand-icon"
        src={`${import.meta.env.BASE_URL}icon.svg`}
        alt="ICEMap"
      />
      <div className="brand-text">
        <h2>{t.appName}</h2>
        <p>{t.appTagline}</p>
      </div>
    </div>
  );
  const legendCard = (
    <div className="legend-section">
      <h3>{t.keyLabel}</h3>
      <ul>
        {LEGEND.map((entry) => (
          <li key={entry.label}>
            <span className="legend-dot" style={{ backgroundColor: entry.color }} />
            {entry.label}
          </li>
        ))}
        <li key="protest">
          <span className="legend-dot" style={{ backgroundColor: PROTEST_COLOR }} />
          {t.protestLegend}
        </li>
        <li key="unrest">
          <span className="legend-dot" style={{ backgroundColor: UNREST_COLOR }} />
          {t.unrestLegend}
        </li>
        <li key="severe">
          <span className="legend-dot" style={{ backgroundColor: SEVERE_COLOR }} />
          {t.severeLegend}
        </li>
      </ul>
    </div>
  );
  const layerControls = (
    <div className="info-card layer-card">
      <p>{t.zoomHint}</p>
      <div className="layer-toggle">
        <span className="layer-icon facility-marker">▲</span>
        <label>
          <input
            type="checkbox"
            checked={showFacilities}
            onChange={() =>
              setShowFacilities((prev) => {
                const next = !prev;
                trackLayerToggle("facilities", next);
                return next;
              })
            }
          />{" "}
          {t.facilitiesLabel} ({FACILITY_COUNT})
        </label>
      </div>
      <div className="layer-toggle">
        <span className="layer-icon child-camp-marker">🧒</span>
        <label>
          <input
            type="checkbox"
            checked={showChildCamps}
            onChange={() =>
              setShowChildCamps((prev) => {
                const next = !prev;
                trackLayerToggle("child_camps", next);
                return next;
              })
            }
          />{" "}
          {t.childCampsLabel} ({CHILD_CAMP_COUNT})
        </label>
      </div>
      <div className="layer-toggle">
        <span className="layer-icon camp-marker">☠</span>
        <label>
          <input
            type="checkbox"
            checked={showDetentionFacilities}
            onChange={() =>
              setShowDetentionFacilities((prev) => {
                const next = !prev;
                trackLayerToggle("detention_facilities", next);
                return next;
              })
            }
          />{" "}
          {t.detentionFacilitiesLabel} ({detentionFacilities.length})
        </label>
      </div>
      <div className="layer-toggle">
        <span className="layer-icon field-office-marker">▲</span>
        <label>
          <input
            type="checkbox"
            checked={showFieldOffices}
            onChange={() =>
              setShowFieldOffices((prev) => {
                const next = !prev;
                trackLayerToggle("field_offices", next);
                return next;
              })
            }
          />{" "}
          {t.fieldOfficesShort} ({FIELD_OFFICES.length})
        </label>
      </div>
    </div>
  );
  const generalCoverageBody =
    groupedGeneralTripletsByDay.length === 0 ? (
      <p>{t.generalCoverageEmpty}</p>
    ) : (
      <div className="general-coverage-list">
        {groupedGeneralTripletsByDay.map((dayGroup) => (
          <section className="day-group" key={`general-${dayGroup.label}`}>
            <div className="day-group-label">{dayGroup.label}</div>
            {dayGroup.items.map((group) => (
              <article className="list-card" key={group.storyId}>
                <div className="list-card-header">
                  <div>
                    <div className="list-card-meta">
                      {t.reportedLabel} {formatPublishedAt(group.publishedAt)}
                    </div>
                    <h3>
                      {group.url ? (
                        <a
                          href={group.url}
                          target="_blank"
                          rel="noreferrer"
                          onClick={handleOutboundClick(
                            group.title,
                            group.url,
                            "general",
                          )}
                        >
                          {group.title}
                        </a>
                      ) : (
                        group.title
                      )}
                    </h3>
                  </div>
                  <div className="badge-group">
                    {group.items.some(isProtestTriplet) && (
                      <span className="badge badge-protest">
                        {t.headlinesPageProtestTag}
                      </span>
                    )}
                    {group.items.some(isSevereTriplet) && (
                      <span className="badge badge-severe">
                        {t.headlinesPageSevereTag}
                      </span>
                    )}
                    {group.items.some(isChildTriplet) && (
                      <span className="badge badge-child">
                        {t.headlinesPageChildTag}
                      </span>
                    )}
                    {group.items.some(isUsNationalEnforcementTriplet) && (
                      <span className="badge badge-us-national">
                        {t.headlinesPageUsNationalTag}
                      </span>
                    )}
                  </div>
                </div>
                <div className="headline-meta">
                  <span className="headline-chip">{getTopicLabel(group.items)}</span>
                  {getLocationLabel(group.items) && (
                    <span className="headline-chip">{getLocationLabel(group.items)}</span>
                  )}
                  <span className="headline-chip">{getImpactLabel(group.items)}</span>
                </div>
                <div className="general-summary">
                  <div className="summary-lines">
                    {getSourceLabels(group.items).map(({ logoKey, fallbackKey, label }) => (
                      <div className="summary-line source-line" key={label}>
                        <SourceLogo
                          primaryKey={logoKey}
                          fallbackKey={fallbackKey}
                          alt={label}
                        />
                        <span>{label}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </article>
            ))}
          </section>
        ))}
      </div>
    );
  const generalPanel = (
    <section className="general-panel">
      <h2>{t.generalCoverage}</h2>
      {generalCoverageBody}
    </section>
  );
  const resourcesView = (
    <div className="resources-view">
      {getResourceSections(language).map((section) => (
        <section key={section.title} className="resource-card">
          <h2>{section.title}</h2>
          <ul>
            {section.links.map((link) => {
              const isAnchor = link.url.startsWith("#");
              const onClick = isAnchor
                ? (event: React.MouseEvent<HTMLAnchorElement>) => {
                    event.preventDefault();
                    if (link.url === "#about") {
                      openAbout();
                    } else if (link.url === "#methodology") {
                      openMethodology();
                    }
                  }
                : () => trackOutboundClick(link.label, link.url, ANALYTICS_PAGE, "resources");
              return (
                <li key={link.url}>
                  <a
                    href={link.url}
                    target={isAnchor ? undefined : "_blank"}
                    rel={isAnchor ? undefined : "noreferrer"}
                    onClick={onClick}
                  >
                    {link.label}
                  </a>
                  {link.description && <p>{link.description}</p>}
                </li>
              );
            })}
          </ul>
        </section>
      ))}
    </div>
  );
  const chartsView = <ChartsView analyticsPage={ANALYTICS_PAGE} />;
  const updatedLabel = (() => {
    const formatted = formatUpdatedAt(lastUpdated);
    if (!formatted) {
      return null;
    }
    return t.dataUpdatedLabel.replace("{timestamp}", formatted);
  })();
  const headerNav = (
    <nav className="site-nav">
      <button
        type="button"
        className={viewMode === "map" ? "active" : ""}
        onClick={openMapView}
      >
        {t.mapTab}
      </button>
      <a
        href={HEADLINES_URL}
        onClick={() => trackNav("headlines", HEADLINES_URL)}
      >
        {t.headlinesTab}
      </a>
      <a
        href={PROTESTS_URL}
        onClick={() => trackNav("protests", PROTESTS_URL)}
      >
        {t.protestsTab}
      </a>
      <button
        type="button"
        className={viewMode === "resources" ? "active" : ""}
        onClick={openResources}
      >
        {t.resourcesTab}
      </button>
      <a
        className={viewMode === "charts" ? "active" : ""}
        href={CHARTS_URL}
        onClick={() => trackNav("charts", CHARTS_URL)}
      >
        {t.chartsTab}
      </a>
      <a href={STATS_URL} onClick={() => trackNav("stats", STATS_URL)}>
        {t.statsTab}
      </a>
      <a href={DEATHS_URL} onClick={() => trackNav("deaths", DEATHS_URL)}>
        Deaths
      </a>
      <button type="button" className="ghost" onClick={openAbout}>
        {ABOUT_CONTENT.title[language]}
      </button>
      <button type="button" className="ghost" onClick={openFeedback}>
        {t.feedbackTab}
      </button>
    </nav>
  );

  return (
    <div className={`map-page viewport-${viewport}`}>
      <PageHeader
        headerClassName="map-header"
        brandClassName="header-title"
        textClassName="header-text"
        title={headerTitle}
        subtitle={headerSummary}
        appName={t.appName}
        updatedLabel={updatedLabel}
        nav={headerNav}
        languageLabel={t.languageLabel}
        language={language}
        languageOptions={LANGUAGE_LABELS}
        onLanguageChange={handleLanguageChange}
        selectId="icemap-language"
        iconSrc={`${import.meta.env.BASE_URL}icon.svg`}
        iconAlt="ICEMap"
      >
        <div className="controls">
          <div className="time-controls">
            {TIME_RANGES.map((range) => (
              <button
                key={range.label.en}
                className={range.value === sinceHours ? "active" : ""}
                onClick={() => handleRangeChange(range.value)}
                type="button"
              >
                {range.label[language]}
              </button>
            ))}
          </div>
          <div className="filter-controls">
            <span className="filter-label">{t.filterLabel}</span>
            {EVENT_FILTERS.map((filter) => (
              <button
                key={filter.value}
                className={filter.value === eventFilter ? "active" : ""}
                onClick={() => handleEventFilterChange(filter.value)}
                type="button"
              >
                {t[filter.labelKey]}
              </button>
            ))}
          </div>
        </div>
      </PageHeader>
      {error && <div className="banner error">{error}</div>}
      {loading && <div className="banner">{t.loading}</div>}
      <div className={`map-content ${isMobile ? "mobile" : ""}`}>
        {viewMode === "resources" ? (
          <div className="resources-view-wrapper">{resourcesView}</div>
        ) : viewMode === "charts" ? (
          <div className="resources-view-wrapper">{chartsView}</div>
        ) : (
          <>
            <div className="map-frame">
              <MapContainer
                center={[39.5, -98.35]}
                zoom={4}
                scrollWheelZoom
                className="leaflet-map"
                whenCreated={(map) => {
                  mapRef.current = map;
                }}
              >
                <MapBounds />
                <TileLayer
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                  url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />
            {groups.map((group) => {
              const count = group.items.length;
              const radius = Math.min(22, 11 + Math.log2(count || 1) * 4);
              const severe = group.items.some(isSevereTriplet);
              const unrest = group.items.some(isUnrestTriplet);
              const protest = group.items.some(isProtestTriplet);
              const color = severe
                ? SEVERE_COLOR
                : unrest
                  ? UNREST_COLOR
                  : protest
                    ? PROTEST_COLOR
                    : getMarkerColor(group.items[0].publishedAt);
              const sortedItems = group.items
                .slice()
                .sort(
                  (a, b) =>
                    new Date(b.publishedAt).getTime() -
                    new Date(a.publishedAt).getTime(),
                );
              const storyGroups = groupByStory(sortedItems);
              const articleCount = storyGroups.length;
              const primary = sortedItems[0];
              const markerHandlers = {
                click() {
                  if (isMobile) {
                    setSelectedGroupKey(group.key);
                    setSelectedFieldOffice(null);
                    setSelectedDetentionFacility(null);
                    setSelectedStaticLocation(null);
                    return;
                  }
                  focusMapOnMarker(group.lat, group.lon);
                },
              };
              return (
                <CircleMarker
                  key={group.key}
                  center={[group.lat, group.lon]}
                  radius={radius}
                  pathOptions={{ color, weight: 1, fillOpacity: 0.7 }}
                  eventHandlers={markerHandlers}
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -12]} opacity={0.95}>
                      {primary ? (
                        <>
                          <div>
                            <strong>
                              {formatSourceLabel(primary.source) ?? t.unknownSource}
                            </strong>
                          </div>
                          <div className="tooltip-title">{primary.title}</div>
                        </>
                      ) : (
                        <div>
                          <strong>
                            {count} {count === 1 ? t.eventSingular : t.eventPlural}
                          </strong>
                        </div>
                      )}
                      {count > 1 && (
                        <div>
                          + {count - 1} {t.moreNearby}
                        </div>
                      )}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup {...popupProps}>
                      <strong>
                        {articleCount}{" "}
                        {articleCount === 1 ? t.articleSingular : t.articlePlural}
                      </strong>
                      <ul className="popup-event-list">
                        {storyGroups.map((story) => (
                          <li key={story.storyId}>
                            <div className="popup-article-title">
                              {story.url ? (
                                <a
                                  href={story.url}
                                  target="_blank"
                                  rel="noreferrer"
                                  onClick={handleOutboundClick(
                                    story.title,
                                    story.url,
                                    "popup",
                                  )}
                                >
                                  {story.title}
                                </a>
                              ) : (
                                story.title
                              )}
                            </div>
                            <div>
                              {t.reportedLabel} {formatPublishedAt(story.publishedAt)}
                            </div>
                            <div className="summary-lines">
                              {getSourceLabels(story.items).map(({ logoKey, fallbackKey, label }) => (
                                <div className="summary-line source-line" key={label}>
                                  <SourceLogo
                                    primaryKey={logoKey}
                                    fallbackKey={fallbackKey}
                                    alt={label}
                                  />
                                  <span>{label}</span>
                                </div>
                              ))}
                            </div>
                          </li>
                        ))}
                      </ul>
                    </Popup>
                  )}
                </CircleMarker>
              );
            })}
            {showFacilities &&
              FACILITY_LOCATIONS.map((loc) => (
                <Marker
                  key={`${loc.name}-facility`}
                  position={[loc.latitude, loc.longitude]}
                  icon={facilityIcon}
                  eventHandlers={{
                    click() {
                      if (isMobile) {
                        setSelectedStaticLocation(loc);
                        setSelectedGroupKey(null);
                        setSelectedFieldOffice(null);
                        setSelectedDetentionFacility(null);
                        return;
                      }
                      focusMapOnMarker(loc.latitude, loc.longitude);
                    },
                  }}
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                      {loc.name}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup {...popupProps}>
                      <strong>{loc.name}</strong>
                      <div>{t.facilitiesLabel}</div>
                      {loc.addressFull && (
                        <div className="popup-address">{loc.addressFull}</div>
                      )}
                      {loc.note && <div>{loc.note}</div>}
                    </Popup>
                  )}
                </Marker>
              ))}
            {showChildCamps &&
              CHILD_CAMP_LOCATIONS.map((loc) => (
                <Marker
                  key={`${loc.name}-childcamp`}
                  position={[loc.latitude, loc.longitude]}
                  icon={childCampIcon}
                  eventHandlers={{
                    click() {
                      if (isMobile) {
                        setSelectedStaticLocation(loc);
                        setSelectedGroupKey(null);
                        setSelectedFieldOffice(null);
                        setSelectedDetentionFacility(null);
                        return;
                      }
                      focusMapOnMarker(loc.latitude, loc.longitude);
                    },
                  }}
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                      {loc.name}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup {...popupProps}>
                      <strong>{loc.name}</strong>
                      <div>{t.unaccompaniedChildrenSite}</div>
                      {loc.addressFull && (
                        <div className="popup-address">{loc.addressFull}</div>
                      )}
                      {loc.note && <div>{loc.note}</div>}
                    </Popup>
                  )}
                </Marker>
              ))}
            {showDetentionFacilities && (
              <Pane name="detentionPane" style={{ zIndex: 350 }}>
                {detentionFacilities.map((facility) => (
                  <Marker
                    key={`${facility.name}-${facility.city}-${facility.state}`}
                    position={[facility.latitude, facility.longitude]}
                    icon={getDetentionIcon(
                      formatPopulationLabel(facility.tracAverageDailyPopulation),
                    )}
                    pane="detentionPane"
                    eventHandlers={{
                      click() {
                        if (isMobile) {
                          setSelectedDetentionFacility(facility);
                          setSelectedGroupKey(null);
                          setSelectedFieldOffice(null);
                          setSelectedStaticLocation(null);
                          return;
                        }
                        focusMapOnMarker(facility.latitude, facility.longitude);
                      },
                    }}
                  >
                    {!isMobile && (
                      <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                        {facility.name}
                      </Tooltip>
                    )}
                    {!isMobile && (
                      <Popup {...popupProps}>
                        <strong>{facility.name}</strong>
                        <div>{t.detentionFacilityType}</div>
                        <div className="popup-address">
                          {facility.addressFull ||
                            `${facility.city}, ${facility.state} ${facility.postalCode}`}
                        </div>
                        {facility.tracTypeDetailed && (
                          <div>
                            {t.tracTypeLabel}: {facility.tracTypeDetailed}
                          </div>
                        )}
                        {facility.tracAverageDailyPopulation && (
                          <div>
                            {t.tracAverageDailyPopulationLabel}: {facility.tracAverageDailyPopulation}
                          </div>
                        )}
                        {facility.tracGuaranteedMinimum && (
                          <div>
                            {t.tracGuaranteedMinimumLabel}: {facility.tracGuaranteedMinimum}
                          </div>
                        )}
                        {facility.tracAsOf && (
                          <div>
                            {t.tracAsOfLabel}: {facility.tracAsOf}
                          </div>
                        )}
                        {facility.detailUrl && (
                          <a
                            href={facility.detailUrl}
                            target="_blank"
                            rel="noreferrer"
                            onClick={handleOutboundClick(
                              facility.name,
                              facility.detailUrl,
                              "facility",
                            )}
                          >
                            {t.viewFacility}
                          </a>
                        )}
                      </Popup>
                    )}
                  </Marker>
                ))}
              </Pane>
            )}
            {showFieldOffices &&
              FIELD_OFFICES.map((office) => (
                <Marker
                  key={`${office.name}-${office.city}-${office.state}`}
                  position={[office.latitude, office.longitude]}
                  icon={fieldOfficeIcon}
                  eventHandlers={{
                    click() {
                      if (isMobile) {
                        setSelectedFieldOffice(office);
                        setSelectedGroupKey(null);
                        setSelectedDetentionFacility(null);
                        setSelectedStaticLocation(null);
                        return;
                      }
                      focusMapOnMarker(office.latitude, office.longitude);
                    },
                  }}
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                      {office.name}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup {...popupProps}>
                      <strong>{office.name}</strong>
                      <div>{t.fieldOfficeType}</div>
                      <div className="popup-address">
                        {office.addressFull || `${office.city}, ${office.state}`}
                      </div>
                    </Popup>
                  )}
                </Marker>
              ))}
          </MapContainer>
        </div>
            {viewport === "desktop" && (
              <aside className="map-side">
                {brandCard}
                {legendCard}
                {layerControls}
              </aside>
            )}
            {isTablet && (
              <div className="map-side tablet-stack">
                {legendCard}
                {layerControls}
              </div>
            )}
          </>
        )}
      </div>
      {!isMobile && viewMode === "map" && generalPanel}
      {isMobile && viewMode === "resources" && (
        <div className="mobile-side-panels resources-mobile">{resourcesView}</div>
      )}
      {isMobile && viewMode === "charts" && (
        <div className="mobile-side-panels resources-mobile">{chartsView}</div>
      )}
      {isMobile && (
        <>
          {viewMode === "map" && (
            <div className="mobile-side-panels">
              {legendCard}
              {layerControls}
              <section className="general-panel mobile">
                <h2>{t.generalCoverage}</h2>
                {generalCoverageBody}
              </section>
            </div>
          )}
          <nav className="bottom-nav">
              <button
                type="button"
                className={viewMode === "map" ? "active" : ""}
                onClick={openMapView}
              >
                <span className="nav-icon">
                  <MapIcon />
                </span>
                <span className="nav-label">{t.mapTab}</span>
              </button>
              <a
                href={HEADLINES_URL}
                onClick={() => trackNav("headlines", HEADLINES_URL)}
              >
                <span className="nav-icon">
                  <HeadlinesIcon />
                </span>
                <span className="nav-label">{t.newsTab}</span>
              </a>
              <a
                href={PROTESTS_URL}
                onClick={() => trackNav("protests", PROTESTS_URL)}
              >
                <span className="nav-icon">
                  <ProtestsIcon />
                </span>
                <span className="nav-label">{t.protestsTab}</span>
              </a>
              <button
                type="button"
                className={viewMode === "resources" ? "active" : ""}
                onClick={openResources}
              >
                <span className="nav-icon">
                  <ResourcesIcon />
                </span>
                <span className="nav-label">
                  {t.resourcesTabMobile ?? t.resourcesTab}
                </span>
              </button>
              <a
                className={viewMode === "charts" ? "active" : ""}
                href={CHARTS_URL}
                onClick={() => trackNav("charts", CHARTS_URL)}
              >
                <span className="nav-icon">
                  <ChartsIcon />
                </span>
                <span className="nav-label">{t.chartsTab}</span>
              </a>
              <a href={STATS_URL} onClick={() => trackNav("stats", STATS_URL)}>
                <span className="nav-icon">
                  <StatsIcon />
                </span>
                <span className="nav-label">{t.statsTab}</span>
              </a>
              <a href={DEATHS_URL} onClick={() => trackNav("deaths", DEATHS_URL)}>
                <span className="nav-icon">
                  <StatsIcon />
                </span>
                <span className="nav-label">Deaths</span>
              </a>
              <button type="button" onClick={openAbout}>
                <span className="nav-icon">
                  <AboutIcon />
                </span>
                <span className="nav-label">{ABOUT_CONTENT.title[language]}</span>
              </button>
              <button type="button" onClick={openFeedback}>
                <span className="nav-icon">
                  <FeedbackIcon />
                </span>
                <span className="nav-label">{t.feedbackTab}</span>
              </button>
          </nav>
        </>
      )}
      {isMobile &&
        viewMode === "map" &&
        (selectedGroup ||
          selectedFieldOffice ||
          selectedDetentionFacility ||
          selectedStaticLocation) && (
          <div className="mobile-event-sheet">
            <div className="mobile-event-sheet-header">
              <div>
                <div className="mobile-event-sheet-meta">
                  {selectedGroup
                    ? `${selectedItems.length} ${
                        selectedItems.length === 1 ? t.eventSingular : t.eventPlural
                      }`
                    : selectedFieldOffice
                      ? t.fieldOfficeType
                      : selectedDetentionFacility
                        ? t.detentionFacilityType
                        : selectedStaticLocation
                          ? getStaticLocationTypeLabel(selectedStaticLocation)
                          : ""}
                </div>
                <h3>
                  {selectedGroup
                    ? selectedLocationLabel
                    : selectedFieldOffice?.name ||
                      selectedDetentionFacility?.name ||
                      selectedStaticLocation?.name}
                </h3>
              </div>
              <button
                type="button"
                className="sheet-close-btn"
                onClick={clearSelection}
                aria-label={t.closeLabel}
              >
                ×
              </button>
            </div>
            {selectedGroup && (
              <ul>
                {selectedItems.map((item) => (
                  <li key={item.story_id + item.who}>
                    <div className="sheet-item-meta">
                      {t.reportedLabel} {formatPublishedAt(item.publishedAt)}
                    </div>
                    <p className="sheet-item-text">
                      {getItemSourceLabel(item)}
                    </p>
                    <p className="sheet-item-title">
                      {item.url ? (
                        <a
                          href={item.url}
                          target="_blank"
                          rel="noreferrer"
                          onClick={handleOutboundClick(item.title, item.url, "sheet")}
                        >
                          {item.title}
                        </a>
                      ) : (
                        item.title
                      )}
                    </p>
                  </li>
                ))}
              </ul>
            )}
            {selectedFieldOffice && !selectedGroup && (
              <div className="sheet-field-office">
                <p className="sheet-item-text">
                  {selectedFieldOffice.addressFull ||
                    `${selectedFieldOffice.city}, ${selectedFieldOffice.state}`}
                </p>
                <p className="sheet-item-meta">
                  {t.coordinatesLabel}: {selectedFieldOffice.latitude.toFixed(3)}, {selectedFieldOffice.longitude.toFixed(3)}
                </p>
              </div>
            )}
            {selectedDetentionFacility && !selectedGroup && !selectedFieldOffice && (
              <div className="sheet-field-office">
                <p className="sheet-item-text">
                  {selectedDetentionFacility.addressFull ||
                    `${selectedDetentionFacility.city}, ${selectedDetentionFacility.state} ${selectedDetentionFacility.postalCode}`}
                </p>
                <p className="sheet-item-meta">
                  {t.coordinatesLabel}: 
                  {selectedDetentionFacility.latitude !== null
                    ? selectedDetentionFacility.latitude.toFixed(3)
                    : "n/a"}
                  {selectedDetentionFacility.longitude !== null
                    ? `, ${selectedDetentionFacility.longitude.toFixed(3)}`
                    : ""}
                </p>
                {selectedDetentionFacility.detailUrl && (
                  <a
                    href={selectedDetentionFacility.detailUrl}
                    target="_blank"
                    rel="noreferrer"
                    onClick={handleOutboundClick(
                      selectedDetentionFacility.name,
                      selectedDetentionFacility.detailUrl,
                      "facility",
                    )}
                  >
                    {t.facilityDetails}
                  </a>
                )}
              </div>
            )}
            {selectedStaticLocation &&
              !selectedGroup &&
              !selectedFieldOffice &&
              !selectedDetentionFacility && (
                <div className="sheet-field-office">
                  {(selectedStaticLocation.addressFull ||
                    selectedStaticLocation.address) && (
                    <p className="sheet-item-text">
                      {selectedStaticLocation.addressFull ||
                        selectedStaticLocation.address}
                    </p>
                  )}
                  {selectedStaticLocation.note && (
                    <p className="sheet-item-text">{selectedStaticLocation.note}</p>
                  )}
                  <p className="sheet-item-meta">
                    {t.coordinatesLabel}: {selectedStaticLocation.latitude.toFixed(3)}, {selectedStaticLocation.longitude.toFixed(3)}
                  </p>
                </div>
              )}
          </div>
        )}
      <footer className="site-footer">
        <p className="site-footer-note">{t.footerDisclaimer}</p>
      </footer>
      {(showAbout || showMethodology) && (
        <div className="modal-backdrop" onClick={closeOverlays}>
          <div
            className="modal-card"
            role="dialog"
            aria-modal="true"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="modal-header">
              <div>
                <h2>
                  {showAbout
                    ? getOverlayText(ABOUT_CONTENT.title, language)
                    : getOverlayText(METHODOLOGY_CONTENT.title, language)}
                </h2>
                <p className="modal-subtitle">
                  {showAbout
                    ? getOverlayText(ABOUT_CONTENT.subtitle ?? { en: "", es: "" }, language)
                    : getOverlayText(
                        METHODOLOGY_CONTENT.subtitle ?? { en: "", es: "" },
                        language,
                      )}
                </p>
              </div>
              <button type="button" className="modal-close" onClick={closeOverlays}>
                ×
              </button>
            </div>
            <div className="modal-body">
              {(showAbout ? ABOUT_CONTENT.sections : METHODOLOGY_CONTENT.sections).map(
                (section) => (
                  <section key={section.title.en}>
                    <h3>{getOverlayText(section.title, language)}</h3>
                    {section.paragraphs?.map((paragraph) => (
                      <p key={paragraph.en}>{getOverlayText(paragraph, language)}</p>
                    ))}
                    {section.listItems && (
                      <ul>
                        {section.listItems.map((item) => (
                          <li key={item.en}>{getOverlayText(item, language)}</li>
                        ))}
                      </ul>
                    )}
                    {section.note && (
                      <div className="modal-note">{getOverlayText(section.note, language)}</div>
                    )}
                  </section>
                ),
              )}
              {showAbout && ABOUT_CONTENT.footer && (
                <p className="modal-footer">{getOverlayText(ABOUT_CONTENT.footer, language)}</p>
              )}
              {showAbout && (
                <div className="modal-actions">
                  <button type="button" className="modal-secondary" onClick={openMethodology}>
                    {METHODOLOGY_CONTENT.title[language]}
                  </button>
                  <a
                    className="modal-secondary"
                    href={FEEDBACK_URL}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {t.feedbackTab}
                  </a>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default TripletsMap;
