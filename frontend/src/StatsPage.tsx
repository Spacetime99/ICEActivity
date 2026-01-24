import { useEffect, useMemo, useState } from "react";
import { STATIC_DATA_BASE_URL } from "./config";
import { ABOUT_CONTENT } from "./overlays";
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
import {
  DEFAULT_LANGUAGE,
  LANGUAGE_LABELS,
  TRANSLATIONS,
  type Language,
} from "./i18n";
import PageHeader from "./PageHeader";
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
  lat: number | null;
  lon: number | null;
  url?: string | null;
  publishedAt: string | null;
  source?: string | null;
  eventTypes?: string[];
  nearFacilityFatality?: boolean;
  nearFacilityKm?: number;
  nearFacilityType?: string;
};

type ArticleFlags = {
  publishedAt: string;
  state: string | null;
  shooting: boolean;
  fatality: boolean;
  injury: boolean;
  child: boolean;
  usNationalEnforcement: boolean;
  redState: boolean;
  blueState: boolean;
  protest: boolean;
  protestMarch: boolean;
  protestStrike: boolean;
  protestVigil: boolean;
  protestDirect: boolean;
  protestUnrest: boolean;
  enforcement: boolean;
  facilityMention: boolean;
  nearFacilityFatality: boolean;
};

type ChartSeries = {
  label: string;
  color: string;
  values: number[];
  total: number;
};

type ChartTick = {
  index: number;
  label: string;
};

type ChartYAxisLabel = {
  value: number;
  label: string;
};

const NAV_BASE_URL = import.meta.env.BASE_URL ?? "/";
const MAP_URL = `${NAV_BASE_URL}`;
const HEADLINES_URL = `${NAV_BASE_URL}headlines.html`;
const PROTESTS_URL = `${NAV_BASE_URL}protests.html`;
const STATS_URL = `${NAV_BASE_URL}stats.html`;
const RESOURCES_URL = `${NAV_BASE_URL}#resources`;
const CHARTS_URL = `${NAV_BASE_URL}#charts`;
const ABOUT_URL = `${NAV_BASE_URL}#about`;
const FEEDBACK_URL = "https://tally.so/r/lbOAvo";

const WEEK_COUNT = 13;
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
const PROTEST_MARCH_TYPES = new Set(["protest", "march", "rally", "demonstration"]);
const PROTEST_STRIKE_TYPES = new Set(["strike", "walkout", "picket"]);
const PROTEST_VIGIL_TYPES = new Set(["vigil"]);
const PROTEST_DIRECT_TYPES = new Set(["sit_in", "occupation", "blockade", "boycott"]);
const PROTEST_UNREST_TYPES = new Set(["riot", "civil_unrest", "uprising", "revolution"]);

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
  pattern: new RegExp(`\\b${state.name.replace(/\\s+/g, "\\\\s+")}\\b`, "i"),
}));
const RED_STATES = new Set([
  "AL",
  "AK",
  "AR",
  "FL",
  "ID",
  "IN",
  "IA",
  "KS",
  "KY",
  "LA",
  "MS",
  "MO",
  "MT",
  "NC",
  "ND",
  "NE",
  "OH",
  "OK",
  "SC",
  "SD",
  "TN",
  "TX",
  "UT",
  "WV",
  "WY",
]);
const BLUE_STATES = new Set([
  "AZ",
  "CA",
  "CO",
  "CT",
  "DC",
  "DE",
  "GA",
  "HI",
  "IL",
  "MA",
  "MD",
  "ME",
  "MI",
  "MN",
  "NH",
  "NJ",
  "NM",
  "NV",
  "NY",
  "OR",
  "PA",
  "RI",
  "VA",
  "VT",
  "WA",
  "WI",
]);

const SHOOTING_KEYWORDS = [
  "shooting",
  "shot",
  "shots fired",
  "gunfire",
  "opened fire",
  "open fire",
];
const FATALITY_KEYWORDS = [
  "fatal",
  "fatality",
  "death",
  "died",
  "dead",
  "killed",
  "killing",
  "murder",
  "murdered",
];
const INJURY_KEYWORDS = [
  "injured",
  "injury",
  "wounded",
  "wound",
  "hurt",
  "hospitalized",
  "hospitalised",
];
const ENFORCEMENT_KEYWORDS = [
  "arrest",
  "arrested",
  "detain",
  "detained",
  "detention",
  "deport",
  "deported",
  "deportation",
  "custody",
  "raid",
  "raids",
  "sweep",
  "operation",
];
const FACILITY_KEYWORDS = [
  "detention center",
  "detention facility",
  "processing center",
  "processing facility",
  "ice facility",
  "ice processing center",
  "shelter",
];

const numberFormatter = new Intl.NumberFormat(undefined);
const percentFormatter = new Intl.NumberFormat(undefined, {
  style: "percent",
  maximumFractionDigits: 1,
});
const TOP_STATE_COLORS = ["#ef4444", "#2563eb", "#16a34a", "#f97316", "#9333ea"];
const weekFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
});
const monthFormatter = new Intl.DateTimeFormat(undefined, { month: "short" });
const dateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});

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

const includesKeyword = (blob: string, keywords: string[]) =>
  keywords.some((keyword) => blob.includes(keyword));
const matchesUsNationalMention = (blob: string) => isUsStatusMention(blob);

const normalizeStateCandidate = (value: string): string | null => {
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
};

const extractState = (whereText: string | null): string | null => {
  if (!whereText) {
    return null;
  }
  const parts = whereText
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length >= 2) {
    const stateCandidate = parts[parts.length - 1];
    const state = normalizeStateCandidate(stateCandidate);
    if (state) {
      return state;
    }
  }
  const lower = whereText.toLowerCase();
  for (const entry of STATE_NAME_PATTERNS) {
    if (entry.pattern.test(lower)) {
      return entry.abbr;
    }
  }
  return null;
};

const isProtestTriplet = (triplet: Triplet) => {
  const types = triplet.eventTypes ?? [];
  return types.some((type) => PROTEST_TYPES.has(type));
};

const hasProtestType = (triplet: Triplet, types: Set<string>) => {
  const eventTypes = triplet.eventTypes ?? [];
  return eventTypes.some((type) => types.has(type));
};

const getWeekStart = (date: Date) => {
  const utc = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  );
  const day = utc.getUTCDay();
  const diff = (day + 6) % 7;
  utc.setUTCDate(utc.getUTCDate() - diff);
  return utc;
};

const buildWeekStarts = (count: number) => {
  const end = getWeekStart(new Date());
  const weeks: Date[] = [];
  for (let offset = count - 1; offset >= 0; offset -= 1) {
    const week = new Date(end);
    week.setUTCDate(end.getUTCDate() - offset * 7);
    weeks.push(week);
  }
  return weeks;
};

const formatWeekLabel = (date: Date) => weekFormatter.format(date);

const buildPath = (
  values: number[],
  width: number,
  height: number,
  xOffset: number,
  yOffset: number,
) => {
  if (values.length === 0) {
    return "";
  }
  const step = values.length > 1 ? width / (values.length - 1) : width;
  return values
    .map((value, index) => {
      const x = xOffset + index * step;
      const y = yOffset + (height - value * height);
      return `${index === 0 ? "M" : "L"}${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
};

const LineChart = ({
  series,
  ticks,
  yMax,
  yLabels,
}: {
  series: ChartSeries[];
  ticks: ChartTick[];
  yMax: number;
  yLabels: ChartYAxisLabel[];
}) => {
  const width = 120;
  const height = 48;
  const chartHeight = 36;
  const topPad = 4;
  const chartTop = topPad;
  const axisY = chartTop + chartHeight;
  const leftPad = 26;
  const chartWidth = width - leftPad;
  const steps = series[0]?.values.length ?? 0;
  const step = steps > 1 ? chartWidth / (steps - 1) : chartWidth;
  const scale = yMax > 0 ? yMax : 1;
  return (
    <svg viewBox={`0 0 ${width} ${height}`} role="img">
      {[0, 0.5, 1].map((value) => {
        const y = chartTop + (chartHeight - value * chartHeight);
        return (
          <line
            key={value}
            className="chart-axis"
            x1={leftPad}
            y1={y}
            x2={width}
            y2={y}
          />
        );
      })}
      {yLabels.map((label) => {
        const ratio = scale > 0 ? label.value / scale : 0;
        const y = chartTop + (chartHeight - ratio * chartHeight);
        return (
          <text
            key={label.label}
            className="chart-y-label"
            x={leftPad - 2}
            y={y}
            textAnchor="end"
            dominantBaseline="middle"
          >
            {label.label}
          </text>
        );
      })}
      {series.map((entry) => (
        <path
          key={entry.label}
          className="chart-line"
          d={buildPath(
            entry.values.map((value) => Math.min(1, value / scale)),
            chartWidth,
            chartHeight,
            leftPad,
            chartTop,
          )}
          stroke={entry.color}
        />
      ))}
      <line className="chart-axis" x1={leftPad} y1={axisY} x2={width} y2={axisY} />
      {steps > 0 &&
        ticks.map((tick) => {
          const x = leftPad + tick.index * step;
          return (
            <text
              key={`${tick.label}-${tick.index}`}
              className="chart-tick"
              x={x}
              y={height}
              textAnchor="middle"
            >
              {tick.label}
            </text>
          );
        })}
    </svg>
  );
};

const StatCard = ({
  title,
  description,
  rangeLabel,
  totalArticles,
  series,
  totalLabel,
  ticks,
  chartLabel,
}: {
  title: string;
  description: string;
  rangeLabel: string;
  totalArticles: number;
  series: ChartSeries[];
  totalLabel: string;
  ticks: ChartTick[];
  chartLabel: string;
}) => {
  let maxValue = 0;
  series.forEach((entry) => {
    entry.values.forEach((value) => {
      if (value > maxValue) {
        maxValue = value;
      }
    });
  });
  const yMax = maxValue || 1;
  const yLabels = maxValue
    ? [
        { value: maxValue, label: percentFormatter.format(maxValue) },
        { value: maxValue / 2, label: percentFormatter.format(maxValue / 2) },
        { value: 0, label: "0%" },
      ]
    : [{ value: 0, label: "0%" }];

  return (
    <section className="stat-card">
      <div className="stat-card-header">
        <h2>{title}</h2>
        <span className="stat-range">{rangeLabel}</span>
      </div>
      <p className="stat-card-description">{description}</p>
      <div className="stat-chart">
        <LineChart series={series} ticks={ticks} yMax={yMax} yLabels={yLabels} />
      </div>
      <div className="stat-chart-caption">{chartLabel}</div>
      <div className="stat-legend">
        {series.map((entry) => (
          <div className="stat-legend-item" key={entry.label}>
            <span className="stat-legend-dot" style={{ backgroundColor: entry.color }} />
            <span>
              {entry.label}: {numberFormatter.format(entry.total)} (
              {totalArticles
                ? percentFormatter.format(entry.total / totalArticles)
                : percentFormatter.format(0)}
              )
            </span>
          </div>
        ))}
      </div>
      <div className="stat-footer">
        {totalLabel}
      </div>
    </section>
  );
};

const StatsPage = () => {
  const [language, setLanguage] = useState<Language>(getInitialLanguage);
  const [triplets, setTriplets] = useState<Triplet[]>([]);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const t = TRANSLATIONS[language];

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("icemap_lang", language);
    }
  }, [language]);

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
        const response = await fetch(`${STATIC_DATA_BASE_URL}/triplets_3mo.json`, {
          signal: controller.signal,
        });
        const payload = await response.text();
        if (!response.ok) {
          throw new Error(`Static data request failed (${response.status})`);
        }
        const json = JSON.parse(payload) as Triplet[];
        setTriplets(json);
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
  }, []);

  const updatedLabel = (() => {
    if (!lastUpdated) {
      return null;
    }
    const parsed = new Date(lastUpdated);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return t.dataUpdatedLabel.replace("{timestamp}", dateFormatter.format(parsed));
  })();

  const stats = useMemo(() => {
    const weeks = buildWeekStarts(WEEK_COUNT);
    const weekLabels = weeks.map((date) => formatWeekLabel(date));
    const weekKeys = weeks.map((date) => date.toISOString().slice(0, 10));
    const weekIndex = new Map<string, number>();
    weekKeys.forEach((key, index) => {
      weekIndex.set(key, index);
    });
    const monthTicks: ChartTick[] = weeks.reduce((acc, date, index) => {
      const isMonthStart = date.getUTCDate() === 1;
      if (index === 0) {
        if (isMonthStart) {
          acc.push({ index, label: monthFormatter.format(date) });
        }
        return acc;
      }
      if (date.getUTCMonth() !== weeks[index - 1].getUTCMonth()) {
        acc.push({ index, label: monthFormatter.format(date) });
      }
      return acc;
    }, [] as ChartTick[]);

    const totals = new Array(weekLabels.length).fill(0);
    const shootingCounts = new Array(weekLabels.length).fill(0);
    const fatalityCounts = new Array(weekLabels.length).fill(0);
    const injuryCounts = new Array(weekLabels.length).fill(0);
    const childCounts = new Array(weekLabels.length).fill(0);
    const usNationalCounts = new Array(weekLabels.length).fill(0);
    const redCounts = new Array(weekLabels.length).fill(0);
    const blueCounts = new Array(weekLabels.length).fill(0);
    const stateCounts = new Map<string, number[]>();
    const protestCounts = new Array(weekLabels.length).fill(0);
    const protestMarchCounts = new Array(weekLabels.length).fill(0);
    const protestStrikeCounts = new Array(weekLabels.length).fill(0);
    const protestVigilCounts = new Array(weekLabels.length).fill(0);
    const protestDirectCounts = new Array(weekLabels.length).fill(0);
    const protestUnrestCounts = new Array(weekLabels.length).fill(0);
    const enforcementCounts = new Array(weekLabels.length).fill(0);
    const facilityCounts = new Array(weekLabels.length).fill(0);
    const nearFacilityFatalityCounts = new Array(weekLabels.length).fill(0);

    const articles = new Map<string, ArticleFlags>();
    triplets.forEach((triplet) => {
      const key = triplet.story_id || triplet.url || triplet.title;
      if (!key || !triplet.publishedAt) {
        return;
      }
      const existing = articles.get(key);
      const rawBlob = buildTripletBlob(triplet);
      const blob = rawBlob.toLowerCase();
      const state = extractState(triplet.where_text);
      const redState = state ? RED_STATES.has(state) : false;
      const blueState = state ? BLUE_STATES.has(state) : false;
      const flags = {
        state,
        shooting: blob ? includesKeyword(blob, SHOOTING_KEYWORDS) : false,
        fatality: blob ? includesKeyword(blob, FATALITY_KEYWORDS) : false,
        injury: blob ? includesKeyword(blob, INJURY_KEYWORDS) : false,
        child: rawBlob ? isChildMention(rawBlob) : false,
        usNationalEnforcement: rawBlob ? matchesUsNationalMention(rawBlob) : false,
        redState,
        blueState,
        enforcement: blob ? includesKeyword(blob, ENFORCEMENT_KEYWORDS) : false,
        facilityMention: blob ? includesKeyword(blob, FACILITY_KEYWORDS) : false,
        protest: isProtestTriplet(triplet),
        protestMarch: hasProtestType(triplet, PROTEST_MARCH_TYPES),
        protestStrike: hasProtestType(triplet, PROTEST_STRIKE_TYPES),
        protestVigil: hasProtestType(triplet, PROTEST_VIGIL_TYPES),
        protestDirect: hasProtestType(triplet, PROTEST_DIRECT_TYPES),
        protestUnrest: hasProtestType(triplet, PROTEST_UNREST_TYPES),
        nearFacilityFatality: Boolean(triplet.nearFacilityFatality),
      };
      if (!existing) {
        articles.set(key, {
          publishedAt: triplet.publishedAt,
          ...flags,
        });
        return;
      }
      if (new Date(triplet.publishedAt) < new Date(existing.publishedAt)) {
        existing.publishedAt = triplet.publishedAt;
      }
      if (!existing.state && flags.state) {
        existing.state = flags.state;
      }
      existing.shooting = existing.shooting || flags.shooting;
      existing.fatality = existing.fatality || flags.fatality;
      existing.injury = existing.injury || flags.injury;
      existing.child = existing.child || flags.child;
      existing.usNationalEnforcement =
        existing.usNationalEnforcement || flags.usNationalEnforcement;
      existing.redState = existing.redState || flags.redState;
      existing.blueState = existing.blueState || flags.blueState;
      existing.protest = existing.protest || flags.protest;
      existing.protestMarch = existing.protestMarch || flags.protestMarch;
      existing.protestStrike = existing.protestStrike || flags.protestStrike;
      existing.protestVigil = existing.protestVigil || flags.protestVigil;
      existing.protestDirect = existing.protestDirect || flags.protestDirect;
      existing.protestUnrest = existing.protestUnrest || flags.protestUnrest;
      existing.enforcement = existing.enforcement || flags.enforcement;
      existing.facilityMention = existing.facilityMention || flags.facilityMention;
      existing.nearFacilityFatality =
        existing.nearFacilityFatality || flags.nearFacilityFatality;
    });

    articles.forEach((article) => {
      const published = new Date(article.publishedAt);
      if (Number.isNaN(published.getTime())) {
        return;
      }
      const weekKey = getWeekStart(published).toISOString().slice(0, 10);
      const index = weekIndex.get(weekKey);
      if (index === undefined) {
        return;
      }
      totals[index] += 1;
      if (article.state) {
        let counts = stateCounts.get(article.state);
        if (!counts) {
          counts = new Array(weekLabels.length).fill(0);
          stateCounts.set(article.state, counts);
        }
        counts[index] += 1;
      }
      if (article.shooting) {
        shootingCounts[index] += 1;
      }
      if (article.fatality) {
        fatalityCounts[index] += 1;
      }
      if (article.injury) {
        injuryCounts[index] += 1;
      }
      if (article.child) {
        childCounts[index] += 1;
      }
      if (article.usNationalEnforcement) {
        usNationalCounts[index] += 1;
      }
      if (article.redState) {
        redCounts[index] += 1;
      }
      if (article.blueState) {
        blueCounts[index] += 1;
      }
      if (article.protest) {
        protestCounts[index] += 1;
      }
      if (article.protestMarch) {
        protestMarchCounts[index] += 1;
      }
      if (article.protestStrike) {
        protestStrikeCounts[index] += 1;
      }
      if (article.protestVigil) {
        protestVigilCounts[index] += 1;
      }
      if (article.protestDirect) {
        protestDirectCounts[index] += 1;
      }
      if (article.protestUnrest) {
        protestUnrestCounts[index] += 1;
      }
      if (article.enforcement) {
        enforcementCounts[index] += 1;
      }
      if (article.facilityMention) {
        facilityCounts[index] += 1;
      }
      if (article.nearFacilityFatality) {
        nearFacilityFatalityCounts[index] += 1;
      }
    });

    const toRatio = (counts: number[]) =>
      counts.map((value, index) =>
        totals[index] >= 5 ? value / totals[index] : 0,
      );

    const stateTotals = Array.from(stateCounts.entries()).map(([state, counts]) => ({
      state,
      total: counts.reduce((sum, value) => sum + value, 0),
    }));
    stateTotals.sort((a, b) => b.total - a.total);
    const topStates = stateTotals.slice(0, 5).map((entry) => entry.state);
    const stateSeries = topStates.map((state) => {
      const counts = stateCounts.get(state) ?? new Array(weekLabels.length).fill(0);
      return {
        state,
        counts,
        ratios: toRatio(counts),
        total: counts.reduce((sum, value) => sum + value, 0),
      };
    });

    return {
      weekLabels,
      rangeLabel: `${weekLabels[0]} - ${weekLabels[weekLabels.length - 1]}`,
      monthTicks,
      totals,
      totalArticles: totals.reduce((sum, value) => sum + value, 0),
      shooting: {
        counts: shootingCounts,
        ratios: toRatio(shootingCounts),
      },
      fatality: {
        counts: fatalityCounts,
        ratios: toRatio(fatalityCounts),
      },
      injury: {
        counts: injuryCounts,
        ratios: toRatio(injuryCounts),
      },
      child: {
        counts: childCounts,
        ratios: toRatio(childCounts),
      },
      usNationalEnforcement: {
        counts: usNationalCounts,
        ratios: toRatio(usNationalCounts),
      },
      redState: {
        counts: redCounts,
        ratios: toRatio(redCounts),
      },
      blueState: {
        counts: blueCounts,
        ratios: toRatio(blueCounts),
      },
      topStates: stateSeries,
      protest: {
        counts: protestCounts,
        ratios: toRatio(protestCounts),
      },
      protestMarch: {
        counts: protestMarchCounts,
        ratios: toRatio(protestMarchCounts),
      },
      protestStrike: {
        counts: protestStrikeCounts,
        ratios: toRatio(protestStrikeCounts),
      },
      protestVigil: {
        counts: protestVigilCounts,
        ratios: toRatio(protestVigilCounts),
      },
      protestDirect: {
        counts: protestDirectCounts,
        ratios: toRatio(protestDirectCounts),
      },
      protestUnrest: {
        counts: protestUnrestCounts,
        ratios: toRatio(protestUnrestCounts),
      },
      enforcement: {
        counts: enforcementCounts,
        ratios: toRatio(enforcementCounts),
      },
      facility: {
        counts: facilityCounts,
        ratios: toRatio(facilityCounts),
      },
      nearFacilityFatality: {
        counts: nearFacilityFatalityCounts,
        ratios: toRatio(nearFacilityFatalityCounts),
      },
    };
  }, [triplets]);

  const headerNav = (
    <nav className="site-nav">
      <a href={MAP_URL}>{t.mapTab}</a>
      <a href={HEADLINES_URL}>{t.headlinesTab}</a>
      <a href={PROTESTS_URL}>{t.protestsTab}</a>
      <a href={RESOURCES_URL}>{t.resourcesTab}</a>
      <a href={CHARTS_URL}>{t.chartsTab}</a>
      <a className="active" href={STATS_URL}>
        {t.statsTab}
      </a>
      <a className="ghost" href={ABOUT_URL}>
        {ABOUT_CONTENT.title[language]}
      </a>
      <a className="ghost" href={FEEDBACK_URL} target="_blank" rel="noreferrer">
        {t.feedbackTab}
      </a>
    </nav>
  );

  const totalArticles = stats.totalArticles;
  const totalLabel = t.statsTotalLabel.replace(
    "{count}",
    numberFormatter.format(totalArticles),
  );
  const ticks = stats.monthTicks;

  const shootingSeries: ChartSeries[] = [
    {
      label: t.statsLegendShootings,
      color: "#ef4444",
      values: stats.shooting.ratios,
      total: stats.shooting.counts.reduce((sum, value) => sum + value, 0),
    },
    {
      label: t.statsLegendFatalities,
      color: "#991b1b",
      values: stats.fatality.ratios,
      total: stats.fatality.counts.reduce((sum, value) => sum + value, 0),
    },
    {
      label: t.statsLegendInjuries,
      color: "#f97316",
      values: stats.injury.ratios,
      total: stats.injury.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const protestSeries: ChartSeries[] = [
    {
      label: t.statsLegendProtest,
      color: "#2f9e44",
      values: stats.protest.ratios,
      total: stats.protest.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const protestMixSeries: ChartSeries[] = [
    {
      label: t.statsLegendProtestMarch,
      color: "#2f9e44",
      values: stats.protestMarch.ratios,
      total: stats.protestMarch.counts.reduce((sum, value) => sum + value, 0),
    },
    {
      label: t.statsLegendProtestStrike,
      color: "#0ea5e9",
      values: stats.protestStrike.ratios,
      total: stats.protestStrike.counts.reduce((sum, value) => sum + value, 0),
    },
    {
      label: t.statsLegendProtestVigil,
      color: "#f59e0b",
      values: stats.protestVigil.ratios,
      total: stats.protestVigil.counts.reduce((sum, value) => sum + value, 0),
    },
    {
      label: t.statsLegendProtestDirect,
      color: "#8b5cf6",
      values: stats.protestDirect.ratios,
      total: stats.protestDirect.counts.reduce((sum, value) => sum + value, 0),
    },
    {
      label: t.statsLegendProtestUnrest,
      color: "#ef4444",
      values: stats.protestUnrest.ratios,
      total: stats.protestUnrest.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const childSeries: ChartSeries[] = [
    {
      label: t.statsLegendChild,
      color: "#eab308",
      values: stats.child.ratios,
      total: stats.child.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const usNationalSeries: ChartSeries[] = [
    {
      label: t.statsLegendUsNational,
      color: "#0ea5e9",
      values: stats.usNationalEnforcement.ratios,
      total: stats.usNationalEnforcement.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const partisanSeries: ChartSeries[] = [
    {
      label: t.statsLegendRedState,
      color: "#ef4444",
      values: stats.redState.ratios,
      total: stats.redState.counts.reduce((sum, value) => sum + value, 0),
    },
    {
      label: t.statsLegendBlueState,
      color: "#2563eb",
      values: stats.blueState.ratios,
      total: stats.blueState.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const topStatesSeries: ChartSeries[] = stats.topStates.map((entry, index) => ({
    label: STATE_ABBR_TO_NAME.get(entry.state) ?? entry.state,
    color: TOP_STATE_COLORS[index % TOP_STATE_COLORS.length],
    values: entry.ratios,
    total: entry.total,
  }));

  const enforcementSeries: ChartSeries[] = [
    {
      label: t.statsLegendEnforcement,
      color: "#1d4ed8",
      values: stats.enforcement.ratios,
      total: stats.enforcement.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const facilitySeries: ChartSeries[] = [
    {
      label: t.statsLegendFacility,
      color: "#0f766e",
      values: stats.facility.ratios,
      total: stats.facility.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  const nearFacilitySeries: ChartSeries[] = [
    {
      label: t.statsLegendNearFacility,
      color: "#7c3aed",
      values: stats.nearFacilityFatality.ratios,
      total: stats.nearFacilityFatality.counts.reduce((sum, value) => sum + value, 0),
    },
  ];

  return (
    <div className="stats-page">
      <PageHeader
        headerClassName="headlines-hero"
        brandClassName="headlines-brand"
        title={t.statsPageTitle}
        subtitle={t.statsPageSubtitle}
        appName={t.appName}
        updatedLabel={updatedLabel}
        nav={headerNav}
        languageLabel={t.languageLabel}
        language={language}
        languageOptions={LANGUAGE_LABELS}
        onLanguageChange={(value) => setLanguage(value)}
        selectId="stats-language"
        iconSrc={`${import.meta.env.BASE_URL}icon.svg`}
        iconAlt="ICEMap"
      />
      <section className="stats-intro">
        <p>{t.statsPageIntro}</p>
        <p className="stats-note">{t.statsPageDisclaimer}</p>
      </section>
      {error && <div className="banner error">{error}</div>}
      {loading && <div className="banner">{t.loading}</div>}
      <section className="stats-grid">
        <StatCard
          title={t.statsMetricViolenceTitle}
          description={t.statsMetricViolenceSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={shootingSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricProtestTitle}
          description={t.statsMetricProtestSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={protestSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricProtestMixTitle}
          description={t.statsMetricProtestMixSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={protestMixSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricChildTitle}
          description={t.statsMetricChildSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={childSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricUsNationalTitle}
          description={t.statsMetricUsNationalSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={usNationalSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricPartisanTitle}
          description={t.statsMetricPartisanSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={partisanSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricTopStatesTitle}
          description={t.statsMetricTopStatesSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={topStatesSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricEnforcementTitle}
          description={t.statsMetricEnforcementSubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={enforcementSeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricFacilityTitle}
          description={t.statsMetricFacilitySubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={facilitySeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
        <StatCard
          title={t.statsMetricNearFacilityTitle}
          description={t.statsMetricNearFacilitySubtitle}
          rangeLabel={stats.rangeLabel}
          totalArticles={totalArticles}
          series={nearFacilitySeries}
          totalLabel={totalLabel}
          ticks={ticks}
          chartLabel={t.statsChartShareLabel}
        />
      </section>
      <nav className="bottom-nav">
        <a href={MAP_URL}>
          <span className="nav-icon">
            <MapIcon />
          </span>
          <span className="nav-label">{t.mapTab}</span>
        </a>
        <a href={HEADLINES_URL}>
          <span className="nav-icon">
            <HeadlinesIcon />
          </span>
          <span className="nav-label">{t.headlinesTab}</span>
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
        <a className="active" href={STATS_URL}>
          <span className="nav-icon">
            <StatsIcon />
          </span>
          <span className="nav-label">{t.statsTab}</span>
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
    </div>
  );
};

export default StatsPage;
