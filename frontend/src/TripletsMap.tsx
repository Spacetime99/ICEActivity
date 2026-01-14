import { useEffect, useMemo, useRef, useState } from "react";
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup,
  useMap,
  Marker,
  Tooltip,
} from "react-leaflet";
import L, { LatLngBoundsExpression } from "leaflet";
import { API_BASE_URL } from "./config";
import { STATIC_LOCATIONS } from "./staticLocations";
import { FIELD_OFFICES } from "./fieldOffices";
import { DETENTION_FACILITIES } from "./detentionFacilities";
import type { DetentionFacility } from "./detentionFacilities";
import { getResourceSections } from "./resources";
import type { FieldOffice } from "./fieldOffices";
import { DEFAULT_LANGUAGE, LANGUAGE_LABELS, TRANSLATIONS, type Language } from "./i18n";
import { CHARTS } from "./charts";
import { ABOUT_CONTENT, METHODOLOGY_CONTENT, getOverlayText } from "./overlays";

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
type ViewMode = "map" | "list" | "resources" | "charts";

// Keep one hour under the API's 90-day upper bound to avoid validation errors on
// deployments that still enforce a strict "< 90 days" check.
const MAX_API_WINDOW_HOURS = 24 * 90 - 1;

const TIME_RANGES: Array<{ label: { en: string; es: string }; value: TimeRangeValue }> = [
  { label: { en: "3d", es: "3d" }, value: 72 },
  { label: { en: "7d", es: "7d" }, value: 24 * 7 },
  { label: { en: "1mo", es: "1 mes" }, value: 24 * 30 },
  { label: { en: "3mo", es: "3 meses" }, value: MAX_API_WINDOW_HOURS },
  { label: { en: "All", es: "Todo" }, value: "all" },
];

const formatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "short",
  timeStyle: "short",
});

const LEGEND = [
  { label: "< 6h", color: "#ff4d4f" },
  { label: "6–24h", color: "#ff7a45" },
  { label: "1–3d", color: "#ffa940" },
  { label: "3–7d", color: "#ffc069" },
  { label: "7–30d", color: "#ffd591" },
];

const SEVERE_COLOR = "#8b0000";
const numberFormatter = new Intl.NumberFormat(undefined);
const FEEDBACK_URL = "https://tally.so/r/lbOAvo";
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

const GENERAL_LOCATION_TOLERANCE = 0.1;
const GENERAL_COORDINATES = { lat: 39.7837304, lon: -100.445882 };
const DC_COORDINATES = { lat: 38.9072, lon: -77.0369 };
const DC_LOCATION_TOLERANCE = 0.15;

const FACILITY_LOCATIONS = STATIC_LOCATIONS.filter((loc) => loc.type === "facility");
const CHILD_CAMP_LOCATIONS = STATIC_LOCATIONS.filter((loc) => loc.type === "child_camp");
const FACILITY_COUNT = FACILITY_LOCATIONS.length;
const CHILD_CAMP_COUNT = CHILD_CAMP_LOCATIONS.length;

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
  const [triplets, setTriplets] = useState<Triplet[]>([]);
  const [generalTriplets, setGeneralTriplets] = useState<Triplet[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showFacilities, setShowFacilities] = useState(true);
  const [showChildCamps, setShowChildCamps] = useState(true);
  const [showFieldOffices, setShowFieldOffices] = useState(true);
  const [showDetentionFacilities, setShowDetentionFacilities] = useState(false);
  const [viewport, setViewport] = useState<"desktop" | "tablet" | "mobile">("desktop");
  const [viewMode, setViewMode] = useState<ViewMode>("map");
  const [selectedGroupKey, setSelectedGroupKey] = useState<string | null>(null);
  const [selectedFieldOffice, setSelectedFieldOffice] = useState<FieldOffice | null>(null);
  const [selectedDetentionFacility, setSelectedDetentionFacility] =
    useState<DetentionFacility | null>(null);
  const [showAbout, setShowAbout] = useState(false);
  const [showMethodology, setShowMethodology] = useState(false);
  const mapRef = useRef<L.Map | null>(null);
  const t = TRANSLATIONS[language];
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
  const formatTripletLine = (item: Triplet) => {
    const who = item.who?.trim();
    const what = item.what?.trim();
    if (who && what) {
      return { label: `${who} ${what}`, hasWho: true, who, what };
    }
    if (who) {
      return { label: who, hasWho: true, who, what: "" };
    }
    if (what) {
      return { label: what, hasWho: false, who: "", what };
    }
    return { label: "", hasWho: false, who: "", what: "" };
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
      if (window.location.hash === "#about") {
        setShowAbout(true);
        setShowMethodology(false);
      } else if (window.location.hash === "#methodology") {
        setShowMethodology(true);
        setShowAbout(false);
      }
    };
    handleHash();
    window.addEventListener("hashchange", handleHash);
    return () => window.removeEventListener("hashchange", handleHash);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    async function fetchTriplets() {
      setLoading(true);
      setError(null);
      try {
        const params = new URLSearchParams();
        if (sinceHours === "all") {
          params.set("since_hours", "0");
        } else {
          params.set("since_hours", String(sinceHours));
        }
        const query = params.toString();
        const response = await fetch(
          `${API_BASE_URL}/api/triplets${query ? `?${query}` : ""}`,
          { signal: controller.signal },
        );
        const payload = await response.text();
        if (!response.ok) {
          let details: string | undefined;
          try {
            const errorJson = JSON.parse(payload);
            details =
              typeof errorJson?.detail === "string"
                ? errorJson.detail
                : JSON.stringify(errorJson);
          } catch {
            details = payload;
          }
          throw new Error(
            `API request failed with status ${response.status}${
              details ? ` — ${details}` : ""
            }`,
          );
        }
        const json = JSON.parse(payload) as Triplet[];
        const partitioned = json.reduce(
          (acc, item) => {
            const whereLower = item.where_text?.toLowerCase() ?? "";
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
            const isGeneral = matchesGeneralText || nearGeneralCenter || nearDc;
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

  const groups = useMemo(() => groupTriplets(triplets), [triplets]);
  const selectedGroup = useMemo(() => {
    if (!selectedGroupKey) {
      return null;
    }
    return groups.find((group) => group.key === selectedGroupKey) ?? null;
  }, [groups, selectedGroupKey]);
  const sortedTriplets = useMemo(
    () =>
      triplets
        .slice()
        .sort(
          (a, b) => new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime(),
        ),
    [triplets],
  );
  const groupedTriplets = useMemo(() => groupByStory(sortedTriplets), [sortedTriplets]);
  const groupedGeneralTriplets = useMemo(
    () => groupByStory(generalTriplets),
    [generalTriplets],
  );
  const totalEvents = triplets.length + generalTriplets.length;
  const activeRangeLabel =
    TIME_RANGES.find((range) => range.value === sinceHours)?.label[language] ?? "custom";

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
        html: "<div>☠</div>",
        iconSize: [30, 30],
        iconAnchor: [15, 15],
      }),
    [],
  );
  const detentionIconCache = useMemo(() => new Map<string, L.DivIcon>(), []);
  const childCampIcon = useMemo(
    () =>
      new L.DivIcon({
        className: "custom-marker child-camp-marker",
        html: "<div>☠</div>",
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
      iconSize: [40, 36],
      iconAnchor: [20, 18],
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
  const openAbout = () => {
    setShowAbout(true);
    setShowMethodology(false);
    if (typeof window !== "undefined") {
      window.history.replaceState(null, "", "#about");
    }
  };
  const openMethodology = () => {
    setShowMethodology(true);
    setShowAbout(false);
    if (typeof window !== "undefined") {
      window.history.replaceState(null, "", "#methodology");
    }
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
    window.open(FEEDBACK_URL, "_blank", "noopener");
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
  };

  useEffect(() => {
    if (!isMobile || viewMode !== "map") {
      setSelectedGroupKey(null);
      setSelectedFieldOffice(null);
      setSelectedDetentionFacility(null);
    }
  }, [isMobile, viewMode]);

  useEffect(() => {
    if (selectedGroupKey && !selectedGroup) {
      setSelectedGroupKey(null);
    }
  }, [selectedGroupKey, selectedGroup]);
  const brandCard = (
    <div className="brand-card">
      <div className="brand-mark">ICE</div>
      <div>
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
            onChange={() => setShowFacilities((prev) => !prev)}
          />{" "}
          {t.facilitiesLabel} ({FACILITY_COUNT})
        </label>
      </div>
      <div className="layer-toggle">
        <span className="layer-icon child-camp-marker">☠</span>
        <label>
          <input
            type="checkbox"
            checked={showChildCamps}
            onChange={() => setShowChildCamps((prev) => !prev)}
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
            onChange={() => setShowDetentionFacilities((prev) => !prev)}
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
            onChange={() => setShowFieldOffices((prev) => !prev)}
          />{" "}
          {t.fieldOfficesShort} ({FIELD_OFFICES.length})
        </label>
      </div>
    </div>
  );
  const generalCoverageBody =
    groupedGeneralTriplets.length === 0 ? (
      <p>{t.generalCoverageEmpty}</p>
    ) : (
      <ul>
        {groupedGeneralTriplets.map((group) => (
          <li key={group.storyId}>
            <div className="general-title">
              {group.url ? (
                <a href={group.url} target="_blank" rel="noreferrer">
                  {group.title}
                </a>
              ) : (
                group.title
              )}
            </div>
            <div>{formatPublishedAt(group.publishedAt)}</div>
            <div className="general-summary">
              <strong>{t.summaryLabel}:</strong>
              <div className="summary-lines">
                {group.items
                  .map((item) => ({ item, line: formatTripletLine(item) }))
                  .filter(({ line }) => line.label)
                  .map(({ item, line }) => (
                    <div className="summary-line" key={item.story_id + item.who + item.what}>
                      {line.hasWho ? (
                        <>
                          <strong>{line.who}</strong> {line.what}
                        </>
                      ) : (
                        line.label
                      )}
                    </div>
                  ))}
              </div>
            </div>
            {group.url && (
              <a href={group.url} target="_blank" rel="noreferrer">
                {t.viewArticle}
              </a>
            )}
          </li>
        ))}
      </ul>
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
                : undefined;
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
  const chartsView = (
    <div className="resources-view">
      {CHARTS.map((chart) => (
        <section key={chart.href} className="resource-card chart-card">
          <h2>{chart.title}</h2>
          <a href={chart.href} target="_blank" rel="noreferrer">
            <img src={chart.imgSrc} alt={chart.imgAlt} />
          </a>
          <p className="chart-credit">
            {chart.creditText}{" "}
            <a href={chart.creditHref} target="_blank" rel="noreferrer">
              Statista
            </a>
            .
          </p>
        </section>
      ))}
    </div>
  );
  const listView = (
    <div className="list-view">
      <h2>{t.latestIncidents}</h2>
      {groupedTriplets.length === 0 ? (
        <p>{t.noIncidents}</p>
      ) : (
        groupedTriplets.slice(0, 200).map((group) => (
          <article className="list-card" key={group.storyId}>
            <div className="list-card-meta">
              {formatPublishedAt(group.publishedAt)}
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
            <div className="general-summary">
              <strong>{t.summaryLabel}:</strong>
              <div className="summary-lines">
                {group.items
                  .map((item) => ({ item, line: formatTripletLine(item) }))
                  .filter(({ line }) => line.label)
                  .map(({ item, line }) => (
                    <div className="summary-line" key={item.story_id + item.who + item.what}>
                      {line.hasWho ? (
                        <>
                          <strong>{line.who}</strong> {line.what}
                        </>
                      ) : (
                        line.label
                      )}
                    </div>
                  ))}
              </div>
            </div>
          </article>
        ))
      )}
      <div className="list-general">
        <h3>{t.generalCoverage}</h3>
        {generalCoverageBody}
      </div>
    </div>
  );
  const viewTabs = (
    <div className="view-tabs">
      <button
        type="button"
        className={viewMode === "map" ? "active" : ""}
        onClick={() => setViewMode("map")}
      >
        {t.mapTab}
      </button>
      <button
        type="button"
        className={viewMode === "list" ? "active" : ""}
        onClick={() => setViewMode("list")}
      >
        {t.headlinesTab}
      </button>
      <button
        type="button"
        className={viewMode === "resources" ? "active" : ""}
        onClick={() => setViewMode("resources")}
      >
        {t.resourcesTab}
      </button>
      <button
        type="button"
        className={viewMode === "charts" ? "active" : ""}
        onClick={() => setViewMode("charts")}
      >
        {t.chartsTab}
      </button>
      <button type="button" className="ghost" onClick={openAbout}>
        {ABOUT_CONTENT.title[language]}
      </button>
      <button type="button" className="ghost" onClick={openFeedback}>
        {t.feedbackTab}
      </button>
    </div>
  );

  return (
    <div className={`map-page viewport-${viewport}`}>
      <header className="map-header">
        <div>
          <h1>{t.appName}</h1>
          <p>
            {t.headerIntro} {t.showingEvents} {totalEvents}{" "}
            {totalEvents === 1 ? t.eventSingular : t.eventPlural}{" "}
            {sinceHours === "all"
              ? t.acrossAllData
              : `${t.fromLast} ${activeRangeLabel}`}
            — {triplets.length} {t.mappedLabel}, {generalTriplets.length}{" "}
            {t.generalCoverageLabel}
          </p>
        </div>
        <div className="controls">
          {TIME_RANGES.map((range) => (
            <button
              key={range.label.en}
              className={range.value === sinceHours ? "active" : ""}
              onClick={() => setSinceHours(range.value)}
              type="button"
            >
              {range.label[language]}
            </button>
          ))}
          <div className="language-select">
            <label htmlFor="icemap-language">{t.languageLabel}</label>
            <select
              id="icemap-language"
              value={language}
              onChange={(event) => setLanguage(event.target.value as Language)}
            >
              {Object.entries(LANGUAGE_LABELS).map(([code, label]) => (
                <option key={code} value={code}>
                  {label}
                </option>
              ))}
            </select>
          </div>
        </div>
      </header>
      {!isMobile && viewTabs}
      {error && <div className="banner error">{error}</div>}
      {loading && <div className="banner">{t.loading}</div>}
      <div className={`map-content ${isMobile ? "mobile" : ""}`}>
        {viewMode === "resources" ? (
          <div className="resources-view-wrapper">{resourcesView}</div>
        ) : viewMode === "charts" ? (
          <div className="resources-view-wrapper">{chartsView}</div>
        ) : viewMode === "list" ? (
          listView
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
              const radius = Math.min(16, 8 + Math.log2(count || 1) * 3);
              const severe = group.items.some(isSevereTriplet);
              const color = severe
                ? SEVERE_COLOR
                : getMarkerColor(group.items[0].publishedAt);
              const participantList = group.items
                .map((item) => item.who)
                .filter(Boolean)
                .slice(0, 5);
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
                            <strong>{primary.who}</strong> {primary.what}
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
                                <a href={story.url} target="_blank" rel="noreferrer">
                                  {story.title}
                                </a>
                              ) : (
                                story.title
                              )}
                            </div>
                            <div>{formatPublishedAt(story.publishedAt)}</div>
                            <div className="summary-lines">
                              {story.items
                                .map((item) => ({ item, line: formatTripletLine(item) }))
                                .filter(({ line }) => line.label)
                                .map(({ item, line }) => (
                                  <div className="summary-line" key={item.story_id + item.who + item.what}>
                                    {line.hasWho ? (
                                      <>
                                        <strong>{line.who}</strong> {line.what}
                                      </>
                                    ) : (
                                      line.label
                                    )}
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
            {showDetentionFacilities &&
              detentionFacilities.map((facility) => (
                <Marker
                  key={`${facility.name}-${facility.city}-${facility.state}`}
                  position={[facility.latitude, facility.longitude]}
                  icon={getDetentionIcon(
                    formatPopulationLabel(facility.tracAverageDailyPopulation),
                  )}
                  eventHandlers={{
                    click() {
                      if (isMobile) {
                        setSelectedDetentionFacility(facility);
                        setSelectedGroupKey(null);
                        setSelectedFieldOffice(null);
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
                        <a href={facility.detailUrl} target="_blank" rel="noreferrer">
                          {t.viewFacility}
                        </a>
                      )}
                    </Popup>
                  )}
                </Marker>
              ))}
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
                onClick={() => setViewMode("map")}
              >
                {t.mapTab}
              </button>
              <button
                type="button"
                className={viewMode === "list" ? "active" : ""}
                onClick={() => setViewMode("list")}
              >
                {t.headlinesTab}
              </button>
              <button
                type="button"
                className={viewMode === "resources" ? "active" : ""}
                onClick={() => setViewMode("resources")}
              >
                {t.resourcesTabMobile ?? t.resourcesTab}
              </button>
              <button
                type="button"
                className={viewMode === "charts" ? "active" : ""}
                onClick={() => setViewMode("charts")}
              >
                {t.chartsTab}
              </button>
              <button type="button" onClick={openAbout}>
                {ABOUT_CONTENT.title[language]}
              </button>
              <button type="button" onClick={openFeedback}>
                {t.feedbackTab}
              </button>
          </nav>
        </>
      )}
      {isMobile &&
        viewMode === "map" &&
        (selectedGroup || selectedFieldOffice || selectedDetentionFacility) && (
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
                      : t.detentionFacilityType}
                </div>
                <h3>
                  {selectedGroup
                    ? selectedLocationLabel
                    : selectedFieldOffice?.name || selectedDetentionFacility?.name}
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
                      {formatPublishedAt(item.publishedAt)}
                    </div>
                    <p className="sheet-item-text">
                      <strong>{item.who}</strong> {item.what}
                    </p>
                    <p className="sheet-item-title">
                      {item.url ? (
                        <a href={item.url} target="_blank" rel="noreferrer">
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
                  <a href={selectedDetentionFacility.detailUrl} target="_blank" rel="noreferrer">
                    {t.facilityDetails}
                  </a>
                )}
              </div>
            )}
          </div>
        )}
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
