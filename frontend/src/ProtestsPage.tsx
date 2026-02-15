import { useEffect, useMemo, useState } from "react";
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
import { STATIC_DATA_BASE_URL } from "./config";
import PageHeader from "./PageHeader";

type LinkItem = {
  label: string;
  url: string;
  note?: string;
};

type Organizer = {
  name: string;
  description: string;
  website: string;
  events: LinkItem[];
};

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

const dateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});

const EVENT_HUBS: LinkItem[] = [
  {
    label: "Mobilize event search",
    url: "https://www.mobilize.us/",
    note: "Search by city or ZIP for public events.",
  },
  {
    label: "MoveOn Mobilize hub",
    url: "https://www.mobilize.us/moveon/",
    note: "Rapid-response actions and local events.",
  },
  {
    label: "Indivisible event listings",
    url: "https://www.indivisible.org/events",
    note: "National and local Indivisible actions.",
  },
];

const ORGANIZERS: Organizer[] = [
  {
    name: "50501 Movement",
    description: "50 protests, 50 states, 1 movement.",
    website: "https://fiftyfifty.one",
    events: [
      { label: "Events page", url: "https://fiftyfifty.one/events" },
      { label: "Mobilize hub", url: "https://www.mobilize.us/50501/" },
    ],
  },
  {
    name: "Indivisible",
    description: "Local chapters organizing community action and defense.",
    website: "https://indivisible.org",
    events: [{ label: "Attend an event", url: "https://www.indivisible.org/events" }],
  },
  {
    name: "MoveOn",
    description: "National rapid-response and issue-based mobilizations.",
    website: "https://moveon.org",
    events: [
      { label: "MoveOn Mobilize hub", url: "https://www.mobilize.us/moveon/" },
      { label: "Press room", url: "https://front.moveon.org/press-room/" },
    ],
  },
  {
    name: "Democratic Socialists of America (DSA)",
    description: "Grassroots actions led by local chapters.",
    website: "https://dsausa.org",
    events: [{ label: "DSA calendar", url: "https://www.dsausa.org/calendar/" }],
  },
  {
    name: "Amnesty International USA",
    description: "Human rights actions and urgent response alerts.",
    website: "https://amnestyusa.org",
    events: [{ label: "Take action center", url: "https://www.amnestyusa.org/take-action/" }],
  },
  {
    name: "Direct Action Everywhere (DxE)",
    description: "Direct action campaigns and community organizing.",
    website: "https://www.directactioneverywhere.com",
    events: [{ label: "Get involved", url: "https://www.directactioneverywhere.com/get-involved" }],
  },
  {
    name: "ACLU",
    description: "Civil liberties actions and local advocacy.",
    website: "https://aclu.org",
    events: [{ label: "ACLU Action Center", url: "https://www.aclu.org/action" }],
  },
  {
    name: "Middletown Pride",
    description: "Regional LGBTQ+ advocacy and community support.",
    website: "https://middletownpride.org",
    events: [{ label: "Official site", url: "https://middletownpride.org" }],
  },
  {
    name: "Free America Walkout",
    description: "Co-organized by 50501 and Women's March.",
    website: "https://freeameri.ca",
    events: [{ label: "Official site", url: "https://freeameri.ca" }],
  },
  {
    name: "No Kings Alliance",
    description: "Coalition resource hub and partner event links.",
    website: "https://nokings.org",
    events: [{ label: "Resource hub", url: "https://nokings.org" }],
  },
];

const ProtestsPage = () => {
  const [language, setLanguage] = useState<Language>(DEFAULT_LANGUAGE);
  const [query, setQuery] = useState("");
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

  const t = TRANSLATIONS[language];

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    const stored = window.localStorage.getItem("icemap_lang");
    if (stored === "en" || stored === "es") {
      setLanguage(stored);
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem("icemap_lang", language);
  }, [language]);

  const handleLanguageChange = (value: Language) => {
    setLanguage(value);
  };

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

  const headerNav = (
    <nav className="site-nav">
      <a href={MAP_URL}>{t.mapTab}</a>
      <a href={HEADLINES_URL}>{t.headlinesTab}</a>
      <a className="active" href={PROTESTS_URL}>
        {t.protestsTab}
      </a>
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

  const filteredOrganizers = useMemo(() => {
    const search = query.trim().toLowerCase();
    if (!search) {
      return ORGANIZERS;
    }
    return ORGANIZERS.filter((org) => {
      const blob = `${org.name} ${org.description}`.toLowerCase();
      return blob.includes(search);
    });
  }, [query]);

  return (
    <div className="protests-page">
      <PageHeader
        headerClassName="headlines-hero"
        brandClassName="headlines-brand"
        title={t.protestsPageTitle}
        subtitle={t.protestsPageSubtitle}
        appName={t.appName}
        updatedLabel={updatedLabel}
        nav={headerNav}
        languageLabel={t.languageLabel}
        language={language}
        languageOptions={LANGUAGE_LABELS}
        onLanguageChange={handleLanguageChange}
        selectId="protests-language"
        iconSrc={`${import.meta.env.BASE_URL}icon.svg`}
        iconAlt="ICEMap"
      />

      <section className="protests-intro">
        <div>
          <h2>{t.protestsPageIntroTitle}</h2>
          <p>{t.protestsPageIntroBody}</p>
        </div>
        <div className="intro-card">
          <h3>{t.protestsPageTipsTitle}</h3>
          <ul>
            <li>{t.protestsPageTipOne}</li>
            <li>{t.protestsPageTipTwo}</li>
            <li>{t.protestsPageTipThree}</li>
            <li>{t.protestsPageTipFour}</li>
          </ul>
        </div>
      </section>

      <section className="filters-card">
        <div className="filters-header">
          <h2>{t.protestsPageSearchTitle}</h2>
          <div className="filter-field">
            <span>{t.protestsPageSearchLabel}</span>
            <input
              type="search"
              placeholder={t.protestsPageSearchPlaceholder}
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          </div>
        </div>
        <div className="event-hubs">
          <h3>{t.protestsPageEventHubs}</h3>
          <div className="hub-grid">
            {EVENT_HUBS.map((hub) => (
              <a key={hub.url} href={hub.url} target="_blank" rel="noreferrer">
                <div className="hub-card">
                  <strong>{hub.label}</strong>
                  {hub.note && <span>{hub.note}</span>}
                </div>
              </a>
            ))}
          </div>
        </div>
      </section>

      <section className="organizer-grid">
        {filteredOrganizers.map((org) => (
          <article className="resource-card organizer-card" key={org.name}>
            <div className="organizer-header">
              <div>
                <h3>
                  <a href={org.website} target="_blank" rel="noreferrer">
                    {org.name}
                  </a>
                </h3>
                <p>{org.description}</p>
              </div>
              <a href={org.website} target="_blank" rel="noreferrer">
                {t.protestsPageOfficialSite}
              </a>
            </div>
            <ul>
              {org.events.map((event) => (
                <li key={event.url}>
                  <a href={event.url} target="_blank" rel="noreferrer">
                    {event.label}
                  </a>
                  {event.note && <p>{event.note}</p>}
                </li>
              ))}
            </ul>
          </article>
        ))}
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
        <a className="active" href={PROTESTS_URL}>
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

export default ProtestsPage;
