import { useEffect, useState } from "react";
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
import ChartsView from "./ChartsView";

const NAV_BASE_URL = import.meta.env.BASE_URL ?? "/";
const MAP_URL = `${NAV_BASE_URL}`;
const HEADLINES_URL = `${NAV_BASE_URL}headlines.html`;
const PROTESTS_URL = `${NAV_BASE_URL}protests.html`;
const STATS_URL = `${NAV_BASE_URL}stats.html`;
const RESOURCES_URL = `${NAV_BASE_URL}#resources`;
const CHARTS_URL = `${NAV_BASE_URL}charts.html`;
const DEATHS_URL = `${NAV_BASE_URL}deaths.html`;
const ABOUT_URL = `${NAV_BASE_URL}#about`;
const FEEDBACK_URL = "https://tally.so/r/lbOAvo";
const ANALYTICS_PAGE = "charts";

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

const ChartsPage = () => {
  const [language, setLanguage] = useState<Language>(getInitialLanguage);
  const t = TRANSLATIONS[language];

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("icemap_lang", language);
    }
  }, [language]);

  const headerNav = (
    <nav className="site-nav">
      <a href={MAP_URL}>{t.mapTab}</a>
      <a href={HEADLINES_URL}>{t.headlinesTab}</a>
      <a href={PROTESTS_URL}>{t.protestsTab}</a>
      <a href={RESOURCES_URL}>{t.resourcesTab}</a>
      <a className="active" href={CHARTS_URL}>
        {t.chartsTab}
      </a>
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
    <div className="charts-page">
      <PageHeader
        headerClassName="headlines-hero"
        brandClassName="headlines-brand"
        title={t.chartsPageTitle}
        subtitle={t.chartsPageSubtitle}
        appName={t.appName}
        nav={headerNav}
        languageLabel={t.languageLabel}
        language={language}
        languageOptions={LANGUAGE_LABELS}
        onLanguageChange={setLanguage}
        selectId="charts-language"
        iconSrc={`${import.meta.env.BASE_URL}icon.svg`}
        iconAlt="ICEMap"
      />
      <section className="charts-content">
        <ChartsView analyticsPage={ANALYTICS_PAGE} />
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
        <a className="active" href={CHARTS_URL}>
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

export default ChartsPage;
