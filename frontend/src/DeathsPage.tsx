import { useEffect, useMemo, useState } from "react";
import { STATIC_DATA_BASE_URL } from "./config";
import PageHeader from "./PageHeader";
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

type Source = {
  url?: string | null;
  source_type?: string | null;
};

type DeathRecord = {
  id?: string;
  person_name?: string | null;
  date_of_death?: string | null;
  death_context?: string | null;
  initial_custody_location?: string | null;
  facility_or_location?: string | null;
  primary_report_url?: string | null;
  sources?: Source[] | null;
};

const NAV_BASE_URL = import.meta.env.BASE_URL ?? "/";
const MAP_URL = `${NAV_BASE_URL}`;
const HEADLINES_URL = `${NAV_BASE_URL}headlines.html`;
const PROTESTS_URL = `${NAV_BASE_URL}protests.html`;
const CHARTS_URL = `${NAV_BASE_URL}charts.html`;
const STATS_URL = `${NAV_BASE_URL}stats.html`;
const DEATHS_URL = `${NAV_BASE_URL}deaths.html`;
const RESOURCES_URL = `${NAV_BASE_URL}#resources`;
const ABOUT_URL = `${NAV_BASE_URL}#about`;
const FEEDBACK_URL = "https://tally.so/r/lbOAvo";

function parseJsonl(text: string): DeathRecord[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as DeathRecord);
}

function parseJson(text: string): DeathRecord[] {
  const payload = JSON.parse(text);
  return Array.isArray(payload) ? (payload as DeathRecord[]) : [];
}

function sourceUrl(record: DeathRecord): string | null {
  const sources = record.sources || [];
  if ((record.death_context || "").toLowerCase() === "detention") {
    const official = sources.find((source) => (source.source_type || "") === "official_report");
    return official?.url || record.primary_report_url || sources[0]?.url || null;
  }
  const news = sources.find((source) => (source.source_type || "") === "news");
  return news?.url || record.primary_report_url || sources[0]?.url || null;
}

function locationLabel(record: DeathRecord): string {
  if ((record.death_context || "").toLowerCase() === "detention") {
    return (
      record.initial_custody_location ||
      record.facility_or_location ||
      "Unknown"
    );
  }
  return record.facility_or_location || "Unknown";
}

const DeathsPage = () => {
  const [language, setLanguage] = useState<Language>(DEFAULT_LANGUAGE);
  const [records, setRecords] = useState<DeathRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const t = TRANSLATIONS[language];

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const stored = window.localStorage.getItem("icemap_lang");
    if (stored === "en" || stored === "es") {
      setLanguage(stored);
      return;
    }
    const browser = navigator.language?.toLowerCase() ?? "";
    if (browser.startsWith("es")) {
      setLanguage("es");
    }
  }, []);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("icemap_lang", language);
    }
  }, [language]);

  useEffect(() => {
    const abort = new AbortController();
    const run = async () => {
      try {
        let response = await fetch(`${STATIC_DATA_BASE_URL}/deaths.json`, {
          signal: abort.signal,
        });
        if (response.ok) {
          const text = await response.text();
          setRecords(parseJson(text));
          return;
        }
        response = await fetch(`${STATIC_DATA_BASE_URL}/deaths.jsonl`, {
          signal: abort.signal,
        });
        if (!response.ok) {
          throw new Error(`Failed to load deaths data (${response.status})`);
        }
        const text = await response.text();
        setRecords(parseJsonl(text));
      } catch (err) {
        if (!abort.signal.aborted) {
          setError(err instanceof Error ? err.message : "Failed to load deaths data.");
        }
      } finally {
        if (!abort.signal.aborted) {
          setLoading(false);
        }
      }
    };
    void run();
    return () => abort.abort();
  }, []);

  const sorted = useMemo(() => {
    return [...records].sort((a, b) => {
      const dateA = a.date_of_death || "";
      const dateB = b.date_of_death || "";
      if (dateA !== dateB) {
        return dateB.localeCompare(dateA);
      }
      return (a.person_name || "").localeCompare(b.person_name || "");
    });
  }, [records]);

  const headerNav = (
    <nav className="site-nav">
      <a href={MAP_URL}>{t.mapTab}</a>
      <a href={HEADLINES_URL}>{t.headlinesTab}</a>
      <a href={PROTESTS_URL}>{t.protestsTab}</a>
      <a href={RESOURCES_URL}>{t.resourcesTab}</a>
      <a href={CHARTS_URL}>{t.chartsTab}</a>
      <a href={STATS_URL}>{t.statsTab}</a>
      <a className="active" href={DEATHS_URL}>Deaths</a>
      <a className="ghost" href={ABOUT_URL}>
        {ABOUT_CONTENT.title[language]}
      </a>
      <a className="ghost" href={FEEDBACK_URL} target="_blank" rel="noreferrer">
        {t.feedbackTab}
      </a>
    </nav>
  );

  return (
    <div className="headlines-page deaths-page">
      <PageHeader
        headerClassName="headlines-hero"
        brandClassName="headlines-brand"
        title="Deaths"
        subtitle="People, detention or incident location, and source link."
        appName="ICEMap"
        nav={headerNav}
        languageLabel={t.languageLabel}
        language={language}
        languageOptions={LANGUAGE_LABELS}
        onLanguageChange={setLanguage}
        selectId="deaths-language"
        iconSrc={`${import.meta.env.BASE_URL}icon.svg`}
        iconAlt="ICEMap"
      />

      <section className="deaths-table-wrap">
        {loading && <p className="loading-text">Loading deaths dataset...</p>}
        {error && <p className="error-text">{error}</p>}
        {!loading && !error && (
          <table className="deaths-table">
            <thead>
              <tr>
                <th>Person</th>
                <th>Context</th>
                <th>Detention / Incident Location</th>
                <th>Date</th>
                <th>Source</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((record) => {
                const url = sourceUrl(record);
                const context = record.death_context || "unknown";
                return (
                  <tr key={record.id || `${record.person_name}-${record.date_of_death}`}>
                    <td>
                      {url ? (
                        <a href={url} target="_blank" rel="noreferrer">
                          {record.person_name || "Unknown"}
                        </a>
                      ) : (
                        record.person_name || "Unknown"
                      )}
                    </td>
                    <td>{context}</td>
                    <td>{locationLabel(record)}</td>
                    <td>{record.date_of_death || "Unknown"}</td>
                    <td>
                      {url ? (
                        <a href={url} target="_blank" rel="noreferrer">
                          {context === "detention" ? "Report" : "News"}
                        </a>
                      ) : (
                        "Unknown"
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
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
        <a className="active" href={DEATHS_URL}>
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
    </div>
  );
};

export default DeathsPage;
