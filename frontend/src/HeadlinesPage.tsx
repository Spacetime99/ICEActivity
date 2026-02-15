import { useEffect, useMemo, useState, type SyntheticEvent } from "react";
import { STATIC_DATA_BASE_URL } from "./config";
import { ABOUT_CONTENT } from "./overlays";
import {
  trackEvent,
  trackFilterChange,
  trackLanguageChange,
  trackLoadMore,
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
import {
  DEFAULT_LANGUAGE,
  LANGUAGE_LABELS,
  TRANSLATIONS,
  type Language,
} from "./i18n";
import {
  buildTripletBlob,
  isChildMention,
  isUsStatusMention,
} from "./mentionPatterns";

type Triplet = {
  story_id: string;
  title: string;
  who: string;
  what: string;
  where_text: string | null;
  lat: number;
  lon: number;
  url?: string | null;
  publishedAt: string | null;
  source?: string | null;
  eventTypes?: string[];
};

type EnrichedTriplet = Triplet & {
  state?: string;
  city?: string;
  isGeneral: boolean;
  sourceLabel?: string;
};

type StoryGroup = {
  storyId: string;
  title: string;
  url?: string | null;
  publishedAt?: string | null;
  items: EnrichedTriplet[];
};

type RangeKey = "3d" | "7d" | "1mo" | "3mo" | "all";
type EventFilter = "all" | "protest" | "unrest" | "severe";
type PrecisionFilter = "all" | "precise" | "general";
type SortOption = "newest" | "oldest" | "coverage";
type FilterCriteria = {
  eventFilter: EventFilter;
  precisionFilter: PrecisionFilter;
  stateFilter: string;
  cityFilter: string;
  sourceFilter: string;
  searchQuery: string;
  fromDate: string;
  toDate: string;
};

const TIME_RANGES: Array<{
  value: RangeKey;
  label: { en: string; es: string };
  summary: { en: string; es: string };
}> = [
  {
    value: "3d",
    label: { en: "3 days", es: "3 dias" },
    summary: { en: "last 3 days", es: "ultimos 3 dias" },
  },
  {
    value: "7d",
    label: { en: "7 days", es: "7 dias" },
    summary: { en: "last 7 days", es: "ultimos 7 dias" },
  },
  {
    value: "1mo",
    label: { en: "1 month", es: "1 mes" },
    summary: { en: "last month", es: "ultimo mes" },
  },
  {
    value: "3mo",
    label: { en: "3 months", es: "3 meses" },
    summary: { en: "last 3 months", es: "ultimos 3 meses" },
  },
  {
    value: "all",
    label: { en: "All time", es: "Todo" },
    summary: { en: "all time", es: "todo el tiempo" },
  },
];

const EVENT_FILTERS: Array<{
  value: EventFilter;
  label: { en: string; es: string };
}> = [
  { value: "all", label: { en: "All events", es: "Todos" } },
  { value: "protest", label: { en: "Protest activity", es: "Protestas" } },
  { value: "unrest", label: { en: "Unrest", es: "Disturbios" } },
  { value: "severe", label: { en: "Severe harm", es: "Graves" } },
];

const PRECISION_FILTERS: Array<{
  value: PrecisionFilter;
  label: { en: string; es: string };
}> = [
  { value: "precise", label: { en: "Precise only", es: "Solo precisas" } },
  { value: "all", label: { en: "All locations", es: "Todas" } },
  { value: "general", label: { en: "General coverage only", es: "Solo general" } },
];

const SORT_OPTIONS: Array<{ value: SortOption; label: { en: string; es: string } }> =
  [
    { value: "newest", label: { en: "Newest first", es: "Mas recientes" } },
    { value: "oldest", label: { en: "Oldest first", es: "Mas antiguos" } },
    { value: "coverage", label: { en: "Most coverage", es: "Mas cobertura" } },
  ];

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

const GENERAL_LOCATION_TOLERANCE = 0.1;
const GENERAL_COORDINATES = { lat: 39.7837304, lon: -100.445882 };
const DC_COORDINATES = { lat: 38.9072, lon: -77.0369 };
const DC_LOCATION_TOLERANCE = 0.15;
const SOURCE_LOGO_EXTS = [".png", ".svg", ".ico"];
const SOURCE_LOGO_BASE = import.meta.env.BASE_URL ?? "/";
const NAV_BASE_URL = import.meta.env.BASE_URL ?? "/";
const HEADLINES_URL = `${NAV_BASE_URL}headlines.html`;
const MAP_URL = `${NAV_BASE_URL}`;
const PROTESTS_URL = `${NAV_BASE_URL}protests.html`;
const STATS_URL = `${NAV_BASE_URL}stats.html`;
const RESOURCES_URL = `${NAV_BASE_URL}#resources`;
const CHARTS_URL = `${NAV_BASE_URL}charts.html`;
const DEATHS_URL = `${NAV_BASE_URL}deaths.html`;
const ABOUT_URL = `${NAV_BASE_URL}#about`;
const FEEDBACK_URL = "https://tally.so/r/lbOAvo";
const ANALYTICS_PAGE = "headlines";

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

const STATES = [
  { name: "Alabama", abbr: "AL" },
  { name: "Alaska", abbr: "AK" },
  { name: "Arizona", abbr: "AZ" },
  { name: "Arkansas", abbr: "AR" },
  { name: "California", abbr: "CA" },
  { name: "Colorado", abbr: "CO" },
  { name: "Connecticut", abbr: "CT" },
  { name: "Delaware", abbr: "DE" },
  { name: "Florida", abbr: "FL" },
  { name: "Georgia", abbr: "GA" },
  { name: "Hawaii", abbr: "HI" },
  { name: "Idaho", abbr: "ID" },
  { name: "Illinois", abbr: "IL" },
  { name: "Indiana", abbr: "IN" },
  { name: "Iowa", abbr: "IA" },
  { name: "Kansas", abbr: "KS" },
  { name: "Kentucky", abbr: "KY" },
  { name: "Louisiana", abbr: "LA" },
  { name: "Maine", abbr: "ME" },
  { name: "Maryland", abbr: "MD" },
  { name: "Massachusetts", abbr: "MA" },
  { name: "Michigan", abbr: "MI" },
  { name: "Minnesota", abbr: "MN" },
  { name: "Mississippi", abbr: "MS" },
  { name: "Missouri", abbr: "MO" },
  { name: "Montana", abbr: "MT" },
  { name: "Nebraska", abbr: "NE" },
  { name: "Nevada", abbr: "NV" },
  { name: "New Hampshire", abbr: "NH" },
  { name: "New Jersey", abbr: "NJ" },
  { name: "New Mexico", abbr: "NM" },
  { name: "New York", abbr: "NY" },
  { name: "North Carolina", abbr: "NC" },
  { name: "North Dakota", abbr: "ND" },
  { name: "Ohio", abbr: "OH" },
  { name: "Oklahoma", abbr: "OK" },
  { name: "Oregon", abbr: "OR" },
  { name: "Pennsylvania", abbr: "PA" },
  { name: "Rhode Island", abbr: "RI" },
  { name: "South Carolina", abbr: "SC" },
  { name: "South Dakota", abbr: "SD" },
  { name: "Tennessee", abbr: "TN" },
  { name: "Texas", abbr: "TX" },
  { name: "Utah", abbr: "UT" },
  { name: "Vermont", abbr: "VT" },
  { name: "Virginia", abbr: "VA" },
  { name: "Washington", abbr: "WA" },
  { name: "West Virginia", abbr: "WV" },
  { name: "Wisconsin", abbr: "WI" },
  { name: "Wyoming", abbr: "WY" },
  { name: "District of Columbia", abbr: "DC" },
];

const STATE_ABBR_SET = new Set(STATES.map((state) => state.abbr));
const STATE_ABBR_TO_NAME = new Map(STATES.map((state) => [state.abbr, state.name]));
const STATE_NAME_PATTERNS = STATES.map((state) => ({
  abbr: state.abbr,
  pattern: new RegExp(`\\b${state.name.replace(/\s+/g, "\\s+")}\\b`, "i"),
}));

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});
const dayFormatter = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" });

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

function formatDate(value?: string | null): string {
  if (!value) {
    return "";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return dateFormatter.format(parsed);
}

function formatDayLabel(value: string | null | undefined, fallback: string): string {
  if (!value) {
    return fallback;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return fallback;
  }
  return dayFormatter.format(parsed);
}

function groupStoryGroupsByDay(
  groups: StoryGroup[],
  fallback: string,
): Array<{ label: string; items: StoryGroup[] }> {
  const grouped: Array<{ label: string; items: StoryGroup[] }> = [];
  const seen = new Map<string, { label: string; items: StoryGroup[] }>();
  groups.forEach((group) => {
    const label = formatDayLabel(group.publishedAt, fallback);
    let entry = seen.get(label);
    if (!entry) {
      entry = { label, items: [] };
      seen.set(label, entry);
      grouped.push(entry);
    }
    entry.items.push(group);
  });
  return grouped;
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

function matchesEventFilter(triplet: Triplet, filter: EventFilter): boolean {
  if (filter === "all") {
    return true;
  }
  if (filter === "protest") {
    return isProtestTriplet(triplet);
  }
  if (filter === "unrest") {
    return isUnrestTriplet(triplet);
  }
  if (filter === "severe") {
    return isSevereTriplet(triplet);
  }
  return true;
}

function isGeneralTriplet(triplet: Triplet): boolean {
  const whereLower = triplet.where_text?.toLowerCase() ?? "";
  const hasCoords = Number.isFinite(triplet.lat) && Number.isFinite(triplet.lon);
  const matchesGeneralText =
    whereLower.includes("united states") ||
    whereLower.includes("washington, dc") ||
    whereLower.includes("washington dc") ||
    whereLower.includes("washington d.c.");
  const nearGeneralCenter =
    Math.abs(triplet.lat - GENERAL_COORDINATES.lat) < GENERAL_LOCATION_TOLERANCE &&
    Math.abs(triplet.lon - GENERAL_COORDINATES.lon) < GENERAL_LOCATION_TOLERANCE;
  const nearDc =
    Math.abs(triplet.lat - DC_COORDINATES.lat) < DC_LOCATION_TOLERANCE &&
    Math.abs(triplet.lon - DC_COORDINATES.lon) < DC_LOCATION_TOLERANCE;
  return !hasCoords || matchesGeneralText || nearGeneralCenter || nearDc;
}

function normalizeStateCandidate(value: string): string | null {
  const cleaned = value.replace(/[^A-Za-z\s]/g, " ").replace(/\s+/g, " ").trim();
  if (!cleaned) {
    return null;
  }
  const upper = cleaned.toUpperCase();
  if (STATE_ABBR_SET.has(upper)) {
    return upper;
  }
  const lower = cleaned.toLowerCase();
  for (const entry of STATE_NAME_PATTERNS) {
    if (entry.pattern.test(lower)) {
      return entry.abbr;
    }
  }
  return null;
}

function normalizeCity(value: string): string | undefined {
  const cleaned = value.replace(/\s+county$/i, "").trim();
  return cleaned || undefined;
}

function extractStateCity(whereText: string | null): { state?: string; city?: string } {
  if (!whereText) {
    return {};
  }
  const parts = whereText
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length >= 2) {
    const stateCandidate = parts[parts.length - 1];
    const state = normalizeStateCandidate(stateCandidate);
    if (state) {
      const cityCandidate = parts.length >= 3 ? parts[parts.length - 2] : parts[0];
      return { state, city: normalizeCity(cityCandidate) };
    }
  }
  const lower = whereText.toLowerCase();
  for (const entry of STATE_NAME_PATTERNS) {
    if (entry.pattern.test(lower)) {
      return { state: entry.abbr };
    }
  }
  return {};
}

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

function getSourceHostname(url?: string | null): string {
  if (!url) {
    return "";
  }
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

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

const SourceLogo = ({
  primaryKey,
  fallbackKey,
  alt,
}: {
  primaryKey?: string | null;
  fallbackKey?: string | null;
  alt: string;
}) => {
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
  const checkTransparency = (event: SyntheticEvent<HTMLImageElement>) => {
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

function groupByStory(data: EnrichedTriplet[]): StoryGroup[] {
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
  return Array.from(groups.values());
}

function getTopicLabel(items: EnrichedTriplet[]): string {
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
  return "Immigration";
}

function getImpactLabel(items: EnrichedTriplet[]): string {
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
  return "Enforcement";
}

function getGroupLocation(items: EnrichedTriplet[]): {
  label: string;
  state?: string;
  city?: string;
} {
  for (const item of items) {
    if (item.city || item.state) {
      const parts = [];
      if (item.city) {
        parts.push(item.city);
      }
      if (item.state) {
        parts.push(STATE_ABBR_TO_NAME.get(item.state) ?? item.state);
      }
      return { label: parts.join(", "), state: item.state, city: item.city };
    }
  }
  for (const item of items) {
    const whereText = item.where_text?.trim();
    if (whereText) {
      return { label: whereText };
    }
  }
  return { label: "" };
}

function parseDateInput(value: string, isEnd: boolean): Date | null {
  if (!value) {
    return null;
  }
  const time = isEnd ? "T23:59:59" : "T00:00:00";
  const parsed = new Date(`${value}${time}`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function formatTemplate(template: string, values: Record<string, string | number>): string {
  return Object.entries(values).reduce(
    (result, [key, value]) => result.replace(`{${key}}`, String(value)),
    template,
  );
}

function filterTriplets(
  items: EnrichedTriplet[],
  criteria: FilterCriteria,
): EnrichedTriplet[] {
  const searchLower = criteria.searchQuery.trim().toLowerCase();
  const fromValue = parseDateInput(criteria.fromDate, false);
  const toValue = parseDateInput(criteria.toDate, true);
  return items.filter((triplet) => {
    if (!matchesEventFilter(triplet, criteria.eventFilter)) {
      return false;
    }
    if (criteria.precisionFilter === "precise" && triplet.isGeneral) {
      return false;
    }
    if (criteria.precisionFilter === "general" && !triplet.isGeneral) {
      return false;
    }
    if (criteria.stateFilter !== "all" && triplet.state !== criteria.stateFilter) {
      return false;
    }
    if (criteria.cityFilter !== "all" && triplet.city !== criteria.cityFilter) {
      return false;
    }
    if (criteria.sourceFilter !== "all" && triplet.sourceLabel !== criteria.sourceFilter) {
      return false;
    }
    if (searchLower) {
      const blob = [
        triplet.title,
        triplet.who,
        triplet.what,
        triplet.where_text,
        triplet.sourceLabel,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      if (!blob.includes(searchLower)) {
        return false;
      }
    }
    if (fromValue || toValue) {
      if (!triplet.publishedAt) {
        return false;
      }
      const published = new Date(triplet.publishedAt);
      if (Number.isNaN(published.getTime())) {
        return false;
      }
      if (fromValue && published < fromValue) {
        return false;
      }
      if (toValue && published > toValue) {
        return false;
      }
    }
    return true;
  });
}

const HeadlinesPage = () => {
  const [language, setLanguage] = useState<Language>(getInitialLanguage);
  const [rangeKey, setRangeKey] = useState<RangeKey>("7d");
  const [eventFilter, setEventFilter] = useState<EventFilter>("all");
  const [precisionFilter, setPrecisionFilter] = useState<PrecisionFilter>("precise");
  const [sortOption, setSortOption] = useState<SortOption>("newest");
  const [searchQuery, setSearchQuery] = useState("");
  const [stateFilter, setStateFilter] = useState("all");
  const [cityFilter, setCityFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [fromDate, setFromDate] = useState("");
  const [toDate, setToDate] = useState("");
  const [triplets, setTriplets] = useState<EnrichedTriplet[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [visibleCount, setVisibleCount] = useState(24);
  const [pulse, setPulse] = useState(false);

  const t = TRANSLATIONS[language];
  const activeRange = TIME_RANGES.find((range) => range.value === rangeKey);
  const activeRangeLabel = activeRange?.summary[language] ?? "";
  const trackNav = (label: string, destination: string) =>
    trackNavClick(label, destination, ANALYTICS_PAGE);

  const handleLanguageChange = (value: Language) => {
    setLanguage(value);
    trackLanguageChange(value, ANALYTICS_PAGE);
  };
  const handleSearchBlur = () => {
    const trimmed = searchQuery.trim();
    trackFilterChange("search", trimmed ? "set" : "cleared", ANALYTICS_PAGE, {
      query_length: trimmed.length,
    });
  };
  const handleRangeChange = (value: RangeKey) => {
    setRangeKey(value);
    trackFilterChange("range", value, ANALYTICS_PAGE);
    if (value !== "all") {
      setFromDate("");
      setToDate("");
    }
  };
  const handleFromDateChange = (value: string) => {
    setFromDate(value);
    trackFilterChange("from_date", value || "cleared", ANALYTICS_PAGE);
    if (value) {
      setRangeKey("all");
      trackFilterChange("range", "all", ANALYTICS_PAGE, { source: "date_input" });
    }
  };
  const handleToDateChange = (value: string) => {
    setToDate(value);
    trackFilterChange("to_date", value || "cleared", ANALYTICS_PAGE);
    if (value) {
      setRangeKey("all");
      trackFilterChange("range", "all", ANALYTICS_PAGE, { source: "date_input" });
    }
  };
  const handleStateChange = (value: string) => {
    setStateFilter(value);
    trackFilterChange("state", value, ANALYTICS_PAGE);
  };
  const handleCityChange = (value: string) => {
    setCityFilter(value);
    trackFilterChange("city", value, ANALYTICS_PAGE);
  };
  const handleSourceChange = (value: string) => {
    setSourceFilter(value);
    trackFilterChange("source", value, ANALYTICS_PAGE);
  };
  const handleEventFilterChange = (value: EventFilter) => {
    setEventFilter(value);
    trackFilterChange("event_filter", value, ANALYTICS_PAGE);
  };
  const handlePrecisionChange = (value: PrecisionFilter) => {
    setPrecisionFilter(value);
    trackFilterChange("precision", value, ANALYTICS_PAGE);
  };
  const handleSortChange = (value: SortOption) => {
    setSortOption(value);
    trackFilterChange("sort", value, ANALYTICS_PAGE);
  };

  const getSourceLabels = (items: EnrichedTriplet[]) => {
    const labels: Array<{
      label: string;
      logoKey: string | null;
      fallbackKey: string | null;
    }> = [];
    const seen = new Set<string>();
    items.forEach((item) => {
      const source = item.source?.trim() ?? "";
      const hostname = getSourceHostname(item.url);
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

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem("icemap_lang", language);
  }, [language]);

  useEffect(() => {
    const controller = new AbortController();
    async function fetchMeta() {
      try {
        const response = await fetch(
          `${STATIC_DATA_BASE_URL}/triplets_meta.json`,
          { signal: controller.signal },
        );
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
    setCityFilter("all");
  }, [stateFilter]);

  useEffect(() => {
    setPulse(true);
    const timer = window.setTimeout(() => setPulse(false), 650);
    return () => window.clearTimeout(timer);
  }, [
    rangeKey,
    eventFilter,
    precisionFilter,
    sortOption,
    searchQuery,
    stateFilter,
    cityFilter,
    sourceFilter,
    fromDate,
    toDate,
  ]);

  useEffect(() => {
    const controller = new AbortController();
    async function fetchTriplets() {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch(
          `${STATIC_DATA_BASE_URL}/triplets_${rangeKey}.json`,
          { signal: controller.signal },
        );
        const payload = await response.text();
        if (!response.ok) {
          throw new Error(`Static data request failed (${response.status})`);
        }
        const json = JSON.parse(payload) as Triplet[];
        const enriched = json.map((item) => {
          const { state, city } = extractStateCity(item.where_text);
          const hostname = getSourceHostname(item.url);
          const sourceValue = hostname || item.source || undefined;
          return {
            ...item,
            state,
            city,
            isGeneral: isGeneralTriplet(item),
            sourceLabel: formatSourceLabel(sourceValue) ?? undefined,
            eventTypes: item.eventTypes ?? [],
          };
        });
        setTriplets(enriched);
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
  }, [rangeKey]);

  useEffect(() => {
    setVisibleCount(24);
  }, [
    rangeKey,
    eventFilter,
    precisionFilter,
    sortOption,
    searchQuery,
    stateFilter,
    cityFilter,
    sourceFilter,
    fromDate,
    toDate,
  ]);

  const filteredTriplets = useMemo(
    () =>
      filterTriplets(triplets, {
        eventFilter,
        precisionFilter,
        stateFilter,
        cityFilter,
        sourceFilter,
        searchQuery,
        fromDate,
        toDate,
      }),
    [
      triplets,
      eventFilter,
      precisionFilter,
      stateFilter,
      cityFilter,
      sourceFilter,
      searchQuery,
      fromDate,
      toDate,
    ],
  );

  const optionBaseTriplets = useMemo(
    () =>
      filterTriplets(triplets, {
        eventFilter,
        precisionFilter,
        stateFilter: "all",
        cityFilter: "all",
        sourceFilter,
        searchQuery,
        fromDate,
        toDate,
      }),
    [
      triplets,
      eventFilter,
      precisionFilter,
      sourceFilter,
      searchQuery,
      fromDate,
      toDate,
    ],
  );

  const groupedTriplets = useMemo(() => {
    const groups = groupByStory(filteredTriplets);
    return groups.sort((a, b) => {
      if (sortOption === "coverage") {
        return b.items.length - a.items.length;
      }
      const aTime = a.publishedAt ? new Date(a.publishedAt).getTime() : 0;
      const bTime = b.publishedAt ? new Date(b.publishedAt).getTime() : 0;
      return sortOption === "oldest" ? aTime - bTime : bTime - aTime;
    });
  }, [filteredTriplets, sortOption]);

  const visibleGroups = useMemo(
    () => groupedTriplets.slice(0, visibleCount),
    [groupedTriplets, visibleCount],
  );
  const dayGroups = useMemo(
    () => groupStoryGroupsByDay(visibleGroups, t.unknownDate),
    [visibleGroups, t.unknownDate],
  );

  useEffect(() => {
    if (visibleCount >= groupedTriplets.length) {
      return;
    }
    const interval = window.setInterval(() => {
      setVisibleCount((prev) => {
        if (prev >= groupedTriplets.length) {
          return prev;
        }
        return Math.min(prev + 24, groupedTriplets.length);
      });
    }, 10000);
    return () => window.clearInterval(interval);
  }, [groupedTriplets.length, visibleCount]);

  const stateOptions = useMemo(() => {
    const present = new Set<string>();
    optionBaseTriplets.forEach((triplet) => {
      if (triplet.state) {
        present.add(triplet.state);
      }
    });
    return STATES.filter((state) => present.has(state.abbr)).sort((a, b) =>
      a.name.localeCompare(b.name),
    );
  }, [optionBaseTriplets]);

  const cityOptions = useMemo(() => {
    const present = new Set<string>();
    optionBaseTriplets.forEach((triplet) => {
      if (stateFilter !== "all" && triplet.state === stateFilter && triplet.city) {
        present.add(triplet.city);
      }
    });
    return Array.from(present.values()).sort((a, b) => a.localeCompare(b));
  }, [optionBaseTriplets, stateFilter]);

  const sourceOptions = useMemo(() => {
    const present = new Set<string>();
    triplets.forEach((triplet) => {
      if (triplet.sourceLabel) {
        present.add(triplet.sourceLabel);
      }
    });
    return Array.from(present.values()).sort((a, b) => a.localeCompare(b));
  }, [triplets]);

  const storyCount = groupedTriplets.length;
  const reportCount = filteredTriplets.length;

  const resetFilters = () => {
    setRangeKey("7d");
    setEventFilter("all");
    setPrecisionFilter("precise");
    setSortOption("newest");
    setSearchQuery("");
    setStateFilter("all");
    setCityFilter("all");
    setSourceFilter("all");
    setFromDate("");
    setToDate("");
  };

  const updatedLabel = (() => {
    const formatted = formatDate(lastUpdated);
    if (!formatted) {
      return null;
    }
    return t.dataUpdatedLabel.replace("{timestamp}", formatted);
  })();

  const headerNav = (
    <nav className="site-nav">
      <a href={MAP_URL}>{t.mapTab}</a>
      <a className="active" href={HEADLINES_URL}>
        {t.headlinesTab}
      </a>
      <a href={PROTESTS_URL}>{t.protestsTab}</a>
      <a href={RESOURCES_URL}>{t.resourcesTab}</a>
      <a href={CHARTS_URL}>{t.chartsTab}</a>
      <a href={STATS_URL}>{t.statsTab}</a>
      <a href={DEATHS_URL}>Deaths</a>
      <a className="ghost" href={ABOUT_URL}>
        {ABOUT_CONTENT.title[language]}
      </a>
      <a className="ghost" href={FEEDBACK_URL} target="_blank" rel="noreferrer">
        {t.feedbackTab}
      </a>
    </nav>
  );

  return (
    <div className="headlines-page">
      <PageHeader
        headerClassName="headlines-hero"
        brandClassName="headlines-brand"
        title={t.headlinesPageTitle}
        subtitle={t.headlinesPageSubtitle}
        appName={t.appName}
        updatedLabel={updatedLabel}
        nav={headerNav}
        languageLabel={t.languageLabel}
        language={language}
        languageOptions={LANGUAGE_LABELS}
        onLanguageChange={handleLanguageChange}
        selectId="headlines-language"
        iconSrc={`${import.meta.env.BASE_URL}icon.svg`}
        iconAlt="ICEMap"
      />

      <section className="headlines-intro">
        <div>
          <h2>{t.headlinesPageIntroTitle}</h2>
          <p>{t.headlinesPageIntroBody}</p>
        </div>
        <div className="intro-card">
          <h3>{t.headlinesPageTipsTitle}</h3>
          <ul>
            <li>{t.headlinesPageTipOne}</li>
            <li>{t.headlinesPageTipTwo}</li>
            <li>{t.headlinesPageTipThree}</li>
          </ul>
        </div>
      </section>

      <section className="filters-card">
        <div className="filters-header">
          <h2>{t.headlinesPageFiltersTitle}</h2>
          <button type="button" onClick={resetFilters} className="ghost">
            {t.headlinesPageReset}
          </button>
        </div>
        <div className="filters-grid">
          <label className="filter-field">
            <span>{t.headlinesPageSearchLabel}</span>
            <input
              type="search"
              placeholder={t.headlinesPageSearchPlaceholder}
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
            />
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageRangeLabel}</span>
            <select
              value={rangeKey}
              onChange={(event) => {
                const value = event.target.value as RangeKey;
                setRangeKey(value);
                if (value !== "all") {
                  setFromDate("");
                  setToDate("");
                }
              }}
            >
              {TIME_RANGES.map((range) => (
                <option key={range.value} value={range.value}>
                  {range.label[language]}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageFromLabel}</span>
            <input
              type="date"
              value={fromDate}
              onChange={(event) => {
                const value = event.target.value;
                setFromDate(value);
                if (value) {
                  setRangeKey("all");
                }
              }}
            />
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageToLabel}</span>
            <input
              type="date"
              value={toDate}
              onChange={(event) => {
                const value = event.target.value;
                setToDate(value);
                if (value) {
                  setRangeKey("all");
                }
              }}
            />
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageStateLabel}</span>
            <select
              value={stateFilter}
              onChange={(event) => setStateFilter(event.target.value)}
            >
              <option value="all">{t.headlinesPageAllStates}</option>
              {stateOptions.map((state) => (
                <option key={state.abbr} value={state.abbr}>
                  {state.name}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageCityLabel}</span>
            <select
              value={cityFilter}
              onChange={(event) => setCityFilter(event.target.value)}
              disabled={stateFilter === "all"}
            >
              <option value="all">{t.headlinesPageAllCities}</option>
              {cityOptions.map((city) => (
                <option key={city} value={city}>
                  {city}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageSourceLabel}</span>
            <select
              value={sourceFilter}
              onChange={(event) => setSourceFilter(event.target.value)}
            >
              <option value="all">{t.headlinesPageAllSources}</option>
              {sourceOptions.map((source) => (
                <option key={source} value={source}>
                  {source}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageEventLabel}</span>
            <select
              value={eventFilter}
              onChange={(event) => setEventFilter(event.target.value as EventFilter)}
            >
              {EVENT_FILTERS.map((filter) => (
                <option key={filter.value} value={filter.value}>
                  {filter.label[language]}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-field">
            <span>{t.headlinesPagePrecisionLabel}</span>
            <select
              value={precisionFilter}
              onChange={(event) => setPrecisionFilter(event.target.value as PrecisionFilter)}
            >
              {PRECISION_FILTERS.map((filter) => (
                <option key={filter.value} value={filter.value}>
                  {filter.label[language]}
                </option>
              ))}
            </select>
          </label>
          <label className="filter-field">
            <span>{t.headlinesPageSortLabel}</span>
            <select
              value={sortOption}
              onChange={(event) => setSortOption(event.target.value as SortOption)}
            >
              {SORT_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label[language]}
                </option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <section
        className={`results-summary ${pulse ? "pulse" : ""}`}
        aria-live="polite"
      >
        <p>
          {formatTemplate(t.headlinesPageResultsSummary, {
            stories: storyCount,
            reports: reportCount,
            range: activeRangeLabel,
          })}
        </p>
        {error && <p className="error-text">{error}</p>}
        {loading && <p className="loading-text">{t.loading}</p>}
      </section>

      <section className={`headlines-list ${pulse ? "pulse" : ""}`}>
        {dayGroups.length === 0 && !loading ? (
          <div className="empty-state">
            <h3>{t.headlinesPageNoResults}</h3>
            <p>{t.headlinesPageNoResultsHint}</p>
          </div>
        ) : (
          dayGroups.map((dayGroup) => (
            <section className="day-group" key={dayGroup.label}>
              <div className="day-group-label">{dayGroup.label}</div>
              {dayGroup.items.map((group) => {
                const location = getGroupLocation(group.items);
                const sourceLabels = getSourceLabels(group.items);
                const severe = group.items.some(isSevereTriplet);
                const child = group.items.some(isChildTriplet);
                const usNational = group.items.some(isUsNationalEnforcementTriplet);
                const unrest = group.items.some(isUnrestTriplet);
                const protest = group.items.some(isProtestTriplet);
                const topic = getTopicLabel(group.items);
                const impact = getImpactLabel(group.items);
                const publishedLabel = formatDate(group.publishedAt);
                return (
                  <article key={group.storyId} className="list-card">
                    <div className="list-card-header">
                      <div>
                        <div className="list-card-meta">
                          {publishedLabel
                            ? `${t.reportedLabel} ${publishedLabel}`
                            : t.unknownDate}
                        </div>
                        <h3>
                          {group.url ? (
                            <a href={group.url} target="_blank" rel="noreferrer">
                              {group.title}
                            </a>
                          ) : (
                            group.title
                          )}
                        </h3>
                      </div>
                      <div className="badge-group">
                        {protest && (
                          <span className="badge badge-protest">
                            {t.headlinesPageProtestTag}
                          </span>
                        )}
                        {unrest && (
                          <span className="badge badge-unrest">
                            {t.headlinesPageUnrestTag}
                          </span>
                        )}
                        {severe && (
                          <span className="badge badge-severe">
                            {t.headlinesPageSevereTag}
                          </span>
                        )}
                        {child && (
                          <span className="badge badge-child">
                            {t.headlinesPageChildTag}
                          </span>
                        )}
                        {usNational && (
                          <span className="badge badge-us-national">
                            {t.headlinesPageUsNationalTag}
                          </span>
                        )}
                        {group.items.some((item) => item.isGeneral) && (
                          <span className="badge badge-general">
                            {t.headlinesPageGeneralTag}
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="headline-meta">
                      {location.label && (
                        <span className="headline-chip">{location.label}</span>
                      )}
                      <span className="headline-chip">{topic}</span>
                      <span className="headline-chip">{impact}</span>
                    </div>
                    <div className="general-summary">
                      <div className="summary-lines">
                        {sourceLabels.map(({ logoKey, fallbackKey, label }) => (
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
                );
              })}
            </section>
          ))
        )}
      </section>

      {visibleCount < groupedTriplets.length && (
        <div className="load-more">
          <button
            type="button"
            onClick={() => setVisibleCount((prev) => prev + 24)}
          >
            {t.headlinesPageLoadMore}
          </button>
        </div>
      )}
      <nav className="bottom-nav">
        <a href={MAP_URL}>
          <span className="nav-icon">
            <MapIcon />
          </span>
          <span className="nav-label">{t.mapTab}</span>
        </a>
        <a className="active" href={HEADLINES_URL}>
          <span className="nav-icon">
            <HeadlinesIcon />
          </span>
          <span className="nav-label">{t.newsTab}</span>
        </a>
        <a href={PROTESTS_URL}>
          <span className="nav-icon">
            <ProtestsIcon />
          </span>
          <span className="nav-label">{t.protestsTab}</span>
        </a>
        <a href={RESOURCES_URL}>
          <span className="nav-icon">
            <ResourcesIcon />
          </span>
          <span className="nav-label">{t.resourcesTabMobile ?? t.resourcesTab}</span>
        </a>
        <a href={CHARTS_URL}>
          <span className="nav-icon">
            <ChartsIcon />
          </span>
          <span className="nav-label">{t.chartsTab}</span>
        </a>
        <a href={STATS_URL}>
          <span className="nav-icon">
            <StatsIcon />
          </span>
          <span className="nav-label">{t.statsTab}</span>
        </a>
        <a href={DEATHS_URL}>
          <span className="nav-icon">
            <StatsIcon />
          </span>
          <span className="nav-label">Deaths</span>
        </a>
        <a href={ABOUT_URL}>
          <span className="nav-icon">
            <AboutIcon />
          </span>
          <span className="nav-label">{ABOUT_CONTENT.title[language]}</span>
        </a>
        <a href={FEEDBACK_URL} target="_blank" rel="noreferrer">
          <span className="nav-icon">
            <FeedbackIcon />
          </span>
          <span className="nav-label">{t.feedbackTab}</span>
        </a>
      </nav>
      <footer className="site-footer">
        <p className="site-footer-note">{t.footerDisclaimer}</p>
      </footer>
    </div>
  );
};

export default HeadlinesPage;
