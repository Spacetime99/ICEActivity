import { useEffect, useMemo, useState } from "react";
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
import { RESOURCE_SECTIONS } from "./resources";
import type { FieldOffice } from "./fieldOffices";

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

type TimeRangeValue = number | "all";
type ViewMode = "map" | "list" | "resources";

// Keep one hour under the API's 90-day upper bound to avoid validation errors on
// deployments that still enforce a strict "< 90 days" check.
const MAX_API_WINDOW_HOURS = 24 * 90 - 1;

const TIME_RANGES: Array<{ label: string; value: TimeRangeValue }> = [
  { label: "3d", value: 72 },
  { label: "7d", value: 24 * 7 },
  { label: "1mo", value: 24 * 30 },
  { label: "3mo", value: MAX_API_WINDOW_HOURS },
  { label: "All", value: "all" },
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

const MapBounds: React.FC = () => {
  const map = useMap();
  const bounds = useMemo(
    () =>
      [
        [24.0, -125.0],
        [50.0, -66.5],
      ] satisfies LatLngBoundsExpression,
    [],
  );
  useEffect(() => {
    map.setMaxBounds(bounds);
    map.setMinZoom(3);
  }, [map, bounds]);
  return null;
};

const TripletsMap = () => {
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
  const totalEvents = triplets.length + generalTriplets.length;
  const activeRangeLabel =
    TIME_RANGES.find((range) => range.value === sinceHours)?.label ?? "custom";

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
        iconSize: [26, 26],
        iconAnchor: [13, 13],
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

  const isMobile = viewport === "mobile";
  const isTablet = viewport === "tablet";
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
        <h2>Incidents Map</h2>
        <p>Interactive overview of ICE-related events.</p>
      </div>
    </div>
  );
  const legendCard = (
    <div className="legend-section">
      <h3>Recency</h3>
      <ul>
        {LEGEND.map((entry) => (
          <li key={entry.label}>
            <span className="legend-dot" style={{ backgroundColor: entry.color }} />
            {entry.label}
          </li>
        ))}
      </ul>
    </div>
  );
  const layerControls = (
    <div className="info-card layer-card">
      <p>
        Zoom + drag to explore. Click a marker to see who/what/where and open the source
        article.
      </p>
      <div className="layer-toggle">
        <span className="layer-icon facility-marker">▲</span>
        <label>
          <input
            type="checkbox"
            checked={showFacilities}
            onChange={() => setShowFacilities((prev) => !prev)}
          />{" "}
          ICE facilities ({FACILITY_COUNT})
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
          Unaccompanied children camps ({CHILD_CAMP_COUNT})
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
          ICE detention facilities ({detentionFacilities.length})
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
          Field offices ({FIELD_OFFICES.length})
        </label>
      </div>
    </div>
  );
  const generalCoverageBody =
    generalTriplets.length === 0 ? (
      <p>No general national coverage in this time range.</p>
    ) : (
      <ul>
        {generalTriplets.map((item) => (
          <li key={item.story_id + item.who}>
            <div className="general-title">
              {item.url ? (
                <a href={item.url} target="_blank" rel="noreferrer">
                  {item.title}
                </a>
              ) : (
                item.title
              )}
            </div>
            {(item.who || item.what) && (
              <div className="general-summary">
                <strong>Summary:</strong>{" "}
                <span>
                  {item.who && <strong>{item.who}</strong>} {item.what}
                </span>
              </div>
            )}
            <div>{formatter.format(new Date(item.publishedAt))}</div>
            {item.url && (
              <a href={item.url} target="_blank" rel="noreferrer">
                View article
              </a>
            )}
          </li>
        ))}
      </ul>
    );
  const generalPanel = (
    <section className="general-panel">
      <h2>General coverage</h2>
      {generalCoverageBody}
    </section>
  );
  const resourcesView = (
    <div className="resources-view">
      {RESOURCE_SECTIONS.map((section) => (
        <section key={section.title} className="resource-card">
          <h2>{section.title}</h2>
          <ul>
            {section.links.map((link) => (
              <li key={link.url}>
                <a href={link.url} target="_blank" rel="noreferrer">
                  {link.label}
                </a>
                {link.description && <p>{link.description}</p>}
              </li>
            ))}
          </ul>
        </section>
      ))}
    </div>
  );
  const listView = (
    <div className="list-view">
      <h2>Latest incidents</h2>
      {sortedTriplets.length === 0 ? (
        <p>No incidents in this window.</p>
      ) : (
        sortedTriplets.slice(0, 200).map((item) => (
          <article className="list-card" key={item.story_id + item.who}>
            <div className="list-card-meta">
              {formatter.format(new Date(item.publishedAt))} •{" "}
              {item.where_text || `${item.lat.toFixed(2)}, ${item.lon.toFixed(2)}`}
            </div>
            <h3>
              {item.url ? (
                <a href={item.url} target="_blank" rel="noreferrer">
                  {item.title}
                </a>
              ) : (
                item.title
              )}
            </h3>
            <p>
              <strong>{item.who}</strong> {item.what}
            </p>
          </article>
        ))
      )}
      <div className="list-general">
        <h3>General coverage</h3>
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
        Map
      </button>
      <button
        type="button"
        className={viewMode === "list" ? "active" : ""}
        onClick={() => setViewMode("list")}
      >
        Headlines
      </button>
      <button
        type="button"
        className={viewMode === "resources" ? "active" : ""}
        onClick={() => setViewMode("resources")}
      >
        Resources
      </button>
    </div>
  );

  return (
    <div className={`map-page viewport-${viewport}`}>
      <header className="map-header">
        <div>
          <h1>ICE Incidents Map</h1>
          <p>
            Showing {totalEvents} events{" "}
            {sinceHours === "all"
              ? "across all ingested data"
              : `from the last ${activeRangeLabel}`}
            — {triplets.length} mapped, {generalTriplets.length} general coverage
          </p>
        </div>
        <div className="controls">
          {TIME_RANGES.map((range) => (
            <button
              key={range.label}
              className={range.value === sinceHours ? "active" : ""}
              onClick={() => setSinceHours(range.value)}
              type="button"
            >
              {range.label}
            </button>
          ))}
        </div>
      </header>
      {!isMobile && viewTabs}
      {error && <div className="banner error">{error}</div>}
      {loading && <div className="banner">Loading…</div>}
      <div className={`map-content ${isMobile ? "mobile" : ""}`}>
        {viewMode === "resources" ? (
          <div className="resources-view-wrapper">{resourcesView}</div>
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
              const primary = sortedItems[0];
              const markerHandlers = isMobile
                ? {
                    click() {
                      setSelectedGroupKey(group.key);
                      setSelectedFieldOffice(null);
                      setSelectedDetentionFacility(null);
                    },
                  }
                : undefined;
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
                          <strong>{count} event{count > 1 ? "s" : ""}</strong>
                        </div>
                      )}
                      {count > 1 && (
                        <div>+ {count - 1} more nearby — click for details.</div>
                      )}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup>
                      <strong>{count} event{count > 1 ? "s" : ""}</strong>
                      <ul className="popup-event-list">
                        {sortedItems.map((item) => (
                          <li key={item.story_id + item.who}>
                            <div>
                              <strong>{item.who}</strong> {item.what}
                            </div>
                            <div className="popup-article-title">
                              {item.url ? (
                                <a href={item.url} target="_blank" rel="noreferrer">
                                  {item.title}
                                </a>
                              ) : (
                                item.title
                              )}
                            </div>
                            <div>{formatter.format(new Date(item.publishedAt))}</div>
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
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                      {loc.name}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup>
                      <strong>{loc.name}</strong>
                      <div>ICE facility</div>
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
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                      {loc.name}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup>
                      <strong>{loc.name}</strong>
                      <div>Unaccompanied children site</div>
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
                  icon={detentionCampIcon}
                  eventHandlers={
                    isMobile
                      ? {
                          click() {
                            setSelectedDetentionFacility(facility);
                            setSelectedGroupKey(null);
                            setSelectedFieldOffice(null);
                          },
                        }
                      : undefined
                  }
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                      {facility.name}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup>
                      <strong>{facility.name}</strong>
                      <div>Detention facility</div>
                      <div className="popup-address">
                        {facility.addressFull ||
                          `${facility.city}, ${facility.state} ${facility.postalCode}`}
                      </div>
                      {facility.detailUrl && (
                        <a href={facility.detailUrl} target="_blank" rel="noreferrer">
                          View facility page
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
                  eventHandlers={
                    isMobile
                      ? {
                          click() {
                            setSelectedFieldOffice(office);
                            setSelectedGroupKey(null);
                          },
                        }
                      : undefined
                  }
                >
                  {!isMobile && (
                    <Tooltip direction="top" offset={[0, -10]} opacity={0.9}>
                      {office.name}
                    </Tooltip>
                  )}
                  {!isMobile && (
                    <Popup>
                      <strong>{office.name}</strong>
                      <div>Field office</div>
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
      {isMobile && (
        <>
          {viewMode === "map" && (
            <div className="mobile-side-panels">
              {legendCard}
              {layerControls}
              <section className="general-panel mobile">
                <h2>General coverage</h2>
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
                Map
              </button>
              <button
                type="button"
                className={viewMode === "list" ? "active" : ""}
                onClick={() => setViewMode("list")}
              >
                Headlines
              </button>
              <button
                type="button"
                className={viewMode === "resources" ? "active" : ""}
                onClick={() => setViewMode("resources")}
              >
                Resources
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
                    ? `${selectedItems.length} event${selectedItems.length === 1 ? "" : "s"}`
                    : selectedFieldOffice
                      ? "Field office"
                      : "Detention facility"}
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
                aria-label="Close details"
              >
                ×
              </button>
            </div>
            {selectedGroup && (
              <ul>
                {selectedItems.map((item) => (
                  <li key={item.story_id + item.who}>
                    <div className="sheet-item-meta">
                      {formatter.format(new Date(item.publishedAt))}
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
                  Coordinates: {selectedFieldOffice.latitude.toFixed(3)}, {selectedFieldOffice.longitude.toFixed(3)}
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
                  Coordinates: 
                  {selectedDetentionFacility.latitude !== null
                    ? selectedDetentionFacility.latitude.toFixed(3)
                    : "n/a"}
                  {selectedDetentionFacility.longitude !== null
                    ? `, ${selectedDetentionFacility.longitude.toFixed(3)}`
                    : ""}
                </p>
                {selectedDetentionFacility.detailUrl && (
                  <a href={selectedDetentionFacility.detailUrl} target="_blank" rel="noreferrer">
                    Facility details
                  </a>
                )}
              </div>
            )}
          </div>
        )}
    </div>
  );
};

export default TripletsMap;
