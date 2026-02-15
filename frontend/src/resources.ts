import type { Language } from "./i18n";

type LocalizedString = {
  en: string;
  es: string;
};

type ResourceLinkData = {
  label: LocalizedString;
  url: string;
  description?: LocalizedString;
};

type ResourceSectionData = {
  title: LocalizedString;
  links: ResourceLinkData[];
};

export type ResourceLink = {
  label: string;
  url: string;
  description?: string;
};

export type ResourceSection = {
  title: string;
  links: ResourceLink[];
};

const RESOURCE_SECTIONS_DATA: ResourceSectionData[] = [
  {
    title: { en: "About This Project", es: "Sobre este proyecto" },
    links: [
      {
        label: { en: "About the map", es: "Sobre el mapa" },
        url: "#about",
        description: {
          en: "Project goals, scope, and a brief overview of the map.",
          es: "Objetivos, alcance y una breve descripcion del mapa.",
        },
      },
      {
        label: {
          en: "Headlines & latest coverage",
          es: "Titulares y cobertura reciente",
        },
        url: "headlines.html",
        description: {
          en: "Dedicated landing page with searchable headlines and location filters.",
          es: "Pagina dedicada con titulares buscables y filtros por ubicacion.",
        },
      },
      {
        label: { en: "Protests & activism", es: "Protestas y activismo" },
        url: "protests.html",
        description: {
          en: "Organizer resources and event hubs for protests and actions.",
          es: "Recursos de organizaciones y hubs de eventos para protestas.",
        },
      },
      {
        label: { en: "Methodology", es: "Metodologia" },
        url: "#methodology",
        description: {
          en: "How reports are collected, summarized, and mapped.",
          es: "Como se recopilan, resumen y geolocalizan los reportes.",
        },
      },
      {
        label: { en: "Feedback", es: "Comentarios" },
        url: "https://tally.so/r/lbOAvo",
        description: {
          en: "Share corrections, suggestions, or data sources.",
          es: "Comparte correcciones, sugerencias o fuentes de datos.",
        },
      },
    ],
  },
  {
    title: {
      en: "Crowdsourced & Community Reporting Maps",
      es: "Mapas comunitarios y colaborativos",
    },
    links: [
      {
        label: { en: "People Over Papers", es: "People Over Papers" },
        url: "https://iceout.org/",
        description: {
          en: "Anonymous crowdsourced pins reporting ICE activity across the U.S.",
          es: "Pins anonimos colaborativos que reportan actividad de ICE en EE.UU.",
        },
      },
      {
        label: {
          en: "Deportation Tracker (ICE Raids Near Me)",
          es: "Deportation Tracker (ICE Raids Near Me)",
        },
        url: "https://deportationtracker.live/report-raid",
        description: {
          en: "Live map of verified and community-reported ICE raids.",
          es: "Mapa en vivo de redadas de ICE verificadas y reportadas por la comunidad.",
        },
      },
      {
        label: { en: "ICE in My Area", es: "ICE in My Area" },
        url: "https://www.iceinmyarea.org/",
        description: {
          en: "Real-time alerts and a map to report ICE vehicles or checkpoints.",
          es: "Alertas en tiempo real y mapa para reportar vehiculos o retenes de ICE.",
        },
      },
    ],
  },
  {
    title: { en: "Data-Driven & Policy Maps", es: "Mapas de datos y politicas" },
    links: [
      {
        label: { en: "The New York Times – Inside the Deportation Machine", es: "The New York Times – Inside the Deportation Machine" },
        url: "https://www.nytimes.com/interactive/2025/12/22/us/trump-immigration-deportation-network-ice-arrests.html",
        description: {
          en: "Interactive map of ICE arrests at jails and community arrests by county.",
          es: "Mapa interactivo de arrestos de ICE en carceles y en la comunidad por condado.",
        },
      },
      {
        label: { en: "The Markup – ICE Cooperation Tracker", es: "The Markup – ICE Cooperation Tracker" },
        url: "https://themarkup.org/tools/2025/04/16/law-enforcement-ice-cooperation-tracker",
        description: {
          en: "Tracks local police agencies with 287(g) agreements or pending requests.",
          es: "Mapa de agencias con acuerdos 287(g) firmados o pendientes.",
        },
      },
      {
        label: { en: "Freedom for Immigrants Detention Map", es: "Freedom for Immigrants Detention Map" },
        url: "https://www.freedomforimmigrants.org/detention-map",
        description: {
          en: "Comprehensive map of immigration detention facilities and impacts.",
          es: "Mapa amplio de centros de detencion y su impacto.",
        },
      },
      {
        label: { en: "ILRC National Map of Local Entanglement", es: "ILRC National Map of Local Entanglement" },
        url: "https://www.ilrc.org/resources/national-map-local-entanglement-ice",
        description: {
          en: "Colors states and counties by how policies limit or assist ICE enforcement.",
          es: "Clasifica estados y condados segun politicas que limitan o apoyan a ICE.",
        },
      },
    ],
  },
  {
    title: { en: "Regional Activity Trackers", es: "Seguimiento regional" },
    links: [
      {
        label: { en: "Newsweek Detention Expansion Map", es: "Newsweek Detention Expansion Map" },
        url: "https://www.newsweek.com/map-shows-new-ice-detention-centers-across-us-11268036",
        description: {
          en: "Visualizes planned and newly opened ICE detention centers.",
          es: "Visualiza centros de detencion planificados y recien abiertos.",
        },
      },
    ],
  },
  {
    title: { en: "Official Data & Dashboards", es: "Datos oficiales y paneles" },
    links: [
      {
        label: { en: "ICE Data Portal", es: "Portal de datos de ICE" },
        url: "https://www.ice.gov/statistics",
        description: {
          en: "Monthly CSVs for book-ins, detained population, removals, and ATD stats.",
          es: "CSV mensuales sobre ingresos, poblacion detenida, expulsiones y ATD.",
        },
      },
      {
        label: {
          en: "DHS Yearbook of Immigration Statistics",
          es: "Anuario de estadisticas migratorias del DHS",
        },
        url: "https://www.dhs.gov/immigration-statistics/yearbook",
        description: {
          en: "Annual reference tables covering arrests, removals, and enforcement outcomes.",
          es: "Tablas anuales sobre arrestos, expulsiones y resultados de control.",
        },
      },
      {
        label: {
          en: "CBP Southwest Land Border Encounters",
          es: "Encuentros en la frontera suroeste (CBP)",
        },
        url: "https://www.cbp.gov/newsroom/stats/southwest-land-border-encounters",
        description: {
          en: "Monthly encounter counts by sector, family unit category, and nationality.",
          es: "Recuentos mensuales por sector, categoria de unidad familiar y nacionalidad.",
        },
      },
      {
        label: { en: "EOIR Adjudication Statistics", es: "Estadisticas de EOIR" },
        url: "https://www.justice.gov/eoir/workload-and-adjudication-statistics",
        description: {
          en: "Immigration court filings, completions, and decision outcomes.",
          es: "Presentaciones, resoluciones y resultados en tribunales migratorios.",
        },
      },
      {
        label: { en: "TRAC Immigration Dashboards", es: "Paneles de TRAC" },
        url: "https://trac.syr.edu/immigration/",
        description: {
          en: "Independent detention, deportation, and court data (download CSVs per chart).",
          es: "Datos independientes de detencion, deportacion y tribunales.",
        },
      },
      {
        label: { en: "TRAC Immigration (reports)", es: "TRAC Immigration (reportes)" },
        url: "https://tracreports.org/immigration/",
        description: {
          en: "TRAC reports portal covering immigration enforcement and courts.",
          es: "Portal de reportes de TRAC sobre control migratorio y tribunales.",
        },
      },
      {
        label: {
          en: "Cato Institute: Deportations would add nearly $1T in costs",
          es: "Cato Institute: Deportaciones sumarian casi $1T en costos",
        },
        url: "https://www.cato.org/blog/deportations-add-almost-1-trillion-costs-gops-big-beautiful-bill",
        description: {
          en: "Policy analysis of projected deportation costs and fiscal impact.",
          es: "Analisis de costos proyectados de deportaciones e impacto fiscal.",
        },
      },
      {
        label: { en: "Statista: Immigration & Migration", es: "Statista: inmigracion y migracion" },
        url: "https://www.statista.com/topics/805/immigration-migration/",
        description: {
          en: "Charts and statistics collection covering migration and immigration policy.",
          es: "Coleccion de graficos y estadisticas sobre migracion y politicas migratorias.",
        },
      },
    ],
  },
  {
    title: {
      en: "ICE — Official & Institutional Sources",
      es: "ICE — fuentes oficiales e institucionales",
    },
    links: [
      {
        label: { en: "History of ICE (ICE.gov)", es: "Historia de ICE (ICE.gov)" },
        url: "https://www.ice.gov/history",
        description: {
          en: "Official overview of ICE history and mission.",
          es: "Resumen oficial de la historia y la mision de ICE.",
        },
      },
      {
        label: { en: "Archived: History of ICE (ICE.gov)", es: "Archivado: Historia de ICE" },
        url: "https://www.ice.gov/features/history",
        description: {
          en: "Archived history page with background context.",
          es: "Pagina archivada con contexto historico.",
        },
      },
      {
        label: {
          en: "ICE Organizational Structure (ICE.gov)",
          es: "Estructura organizativa de ICE (ICE.gov)",
        },
        url: "https://www.ice.gov/leadership/organizational-structure",
        description: {
          en: "Org chart and leadership structure of ICE.",
          es: "Organigrama y estructura de liderazgo de ICE.",
        },
      },
      {
        label: {
          en: "Enforcement and Removal Operations (ICE.gov)",
          es: "Operaciones de control y remocion (ICE.gov)",
        },
        url: "https://www.ice.gov/about-ice/ero",
        description: {
          en: "Overview of ERO responsibilities and scope.",
          es: "Resumen de responsabilidades y alcance de ERO.",
        },
      },
      {
        label: {
          en: "Creation of the Department of Homeland Security (DHS.gov)",
          es: "Creacion del Departamento de Seguridad Nacional (DHS.gov)",
        },
        url: "https://www.dhs.gov/creation-department-homeland-security",
        description: {
          en: "Background on DHS formation after 9/11.",
          es: "Contexto sobre la creacion del DHS tras 9/11.",
        },
      },
      {
        label: {
          en: "Immigration Enforcement (DHS OHSS stats & definitions)",
          es: "Control migratorio (DHS OHSS estadisticas y definiciones)",
        },
        url: "https://ohss.dhs.gov/topics/immigration/immigration-enforcement",
        description: {
          en: "Definitions and statistical context from DHS OHSS.",
          es: "Definiciones y contexto estadistico de DHS OHSS.",
        },
      },
    ],
  },
  {
    title: {
      en: "ICE — Neutral Explainers & Context",
      es: "ICE — explicadores neutrales y contexto",
    },
    links: [
      {
        label: { en: "What is ICE and what does it do? (USAFacts)", es: "Que es ICE y que hace? (USAFacts)" },
        url: "https://usafacts.org/articles/what-is-ice-and-what-does-it-do/",
        description: {
          en: "Plain-language explainer on ICE responsibilities and scope.",
          es: "Explicacion clara sobre funciones y alcance de ICE.",
        },
      },
      {
        label: {
          en: "What does ICE do? (USAFacts subagency explainer)",
          es: "Que hace ICE? (USAFacts explicador de subagencia)",
        },
        url: "https://usafacts.org/explainers/what-does-the-us-government-do/subagency/us-immigration-and-customs-enforcement/",
        description: {
          en: "Overview of ICE within the US government structure.",
          es: "Resumen de ICE dentro de la estructura del gobierno de EE.UU.",
        },
      },
      {
        label: { en: "Why ICE was created (TIME)", es: "Por que se creo ICE (TIME)" },
        url: "https://time.com/5325492/abolish-ice-history/",
        description: {
          en: "Historical context for ICE creation and policy shifts.",
          es: "Contexto historico sobre la creacion de ICE y cambios politicos.",
        },
      },
    ],
  },
  {
    title: {
      en: "Fascism vs Trump — Serious Frameworks",
      es: "Fascismo vs Trump — marcos serios",
    },
    links: [
      {
        label: { en: "Timothy Snyder — On Tyranny (official page)", es: "Timothy Snyder — On Tyranny (oficial)" },
        url: "https://timothysnyder.org/on-tyranny",
        description: {
          en: "Author background and framework for understanding authoritarianism.",
          es: "Marco de referencia sobre autoritarismo del autor.",
        },
      },
      {
        label: {
          en: "Vox interview: \"Post-truth is pre-fascism\" (Snyder)",
          es: "Entrevista Vox: \"Post-truth is pre-fascism\" (Snyder)",
        },
        url: "https://www.vox.com/conversations/2017/3/9/14838088/donald-trump-fascism-europe-history-totalitarianism-post-truth",
        description: {
          en: "Interview on historical framing and political language.",
          es: "Entrevista sobre marco historico y lenguaje politico.",
        },
      },
      {
        label: {
          en: "The New Yorker: What Does It Mean That Donald Trump Is a Fascist?",
          es: "The New Yorker: Que significa que Donald Trump sea fascista?",
        },
        url: "https://www.newyorker.com/magazine/dispatches/what-does-it-mean-that-donald-trump-is-a-fascist",
        description: {
          en: "Essay on definitions and historical comparisons.",
          es: "Ensayo sobre definiciones y comparaciones historicas.",
        },
      },
      {
        label: {
          en: "The New Yorker: Why We Can’t Stop Arguing About Whether Trump Is a Fascist",
          es: "The New Yorker: Por que no dejamos de debatir si Trump es fascista",
        },
        url: "https://www.newyorker.com/books/under-review/why-we-cant-stop-arguing-about-whether-trump-is-a-fascist",
        description: {
          en: "Review essay on debate and framing.",
          es: "Ensayo sobre el debate y el marco conceptual.",
        },
      },
    ],
  },
  {
    title: {
      en: "Immigration Enforcement History",
      es: "Historia del control migratorio",
    },
    links: [
      {
        label: { en: "The Deportation Machine (book summary)", es: "The Deportation Machine (resumen)" },
        url: "https://www.abbeys.com.au/book/the-deportation-machine-americas-long-history-of-expelling-immigrants-9780691204208.do",
        description: {
          en: "Book listing and summary on long-term enforcement history.",
          es: "Resumen y ficha del libro sobre historia de la expulsion.",
        },
      },
    ],
  },
  {
    title: { en: "Commentary & Debate", es: "Comentario y debate" },
    links: [
      {
        label: { en: "Guardian: Is Donald Trump a fascist?", es: "Guardian: Trump es fascista?" },
        url: "https://www.theguardian.com/us-news/2024/sep/21/is-donald-trump-a-fascist",
        description: {
          en: "Opinion and debate coverage on political labeling.",
          es: "Cobertura de opinion y debate sobre etiquetas politicas.",
        },
      },
    ],
  },
  {
    title: { en: "Facilities & Detainee Lookup", es: "Instalaciones y localizacion" },
    links: [
      {
        label: {
          en: "ICE Detention Facilities Directory",
          es: "Directorio de centros de detencion de ICE",
        },
        url: "https://www.ice.gov/detain/detention-facilities",
        description: {
          en: "Official facility addresses, field office contacts, and phone numbers.",
          es: "Direcciones oficiales, contactos de oficinas de campo y telefonos.",
        },
      },
      {
        label: { en: "ICE Detainee Locator", es: "Localizador de detenidos de ICE" },
        url: "https://locator.ice.gov/odls/#/index",
        description: {
          en: "Search for individuals currently in ICE custody.",
          es: "Busqueda de personas actualmente bajo custodia de ICE.",
        },
      },
      {
        label: { en: "OPLA Field Office Contacts", es: "Contactos OPLA" },
        url: "https://www.ice.gov/contact/opla",
        description: {
          en: "Regional Office of the Principal Legal Advisor contact information.",
          es: "Contactos regionales de la Oficina del Asesor Juridico Principal.",
        },
      },
    ],
  },
  {
    title: {
      en: "Know Your Rights & Community Guides",
      es: "Conozca sus derechos y guias comunitarias",
    },
    links: [
      {
        label: { en: "ACLU – Immigrants' Rights", es: "ACLU – Derechos de inmigrantes" },
        url: "https://www.aclu.org/know-your-rights/immigrants-rights",
        description: {
          en: "Steps to take if approached by ICE, including right-to-remain-silent guidance.",
          es: "Pasos a seguir si ICE se acerca, incluido el derecho a guardar silencio.",
        },
      },
      {
        label: {
          en: "Immigrant Legal Resource Center",
          es: "Centro de Recursos Legales para Inmigrantes",
        },
        url: "https://www.ilrc.org/community-resources",
        description: {
          en: "Community tip sheets on ICE encounters and raid preparedness.",
          es: "Guias comunitarias sobre encuentros con ICE y preparacion ante redadas.",
        },
      },
      {
        label: {
          en: "National Immigration Law Center",
          es: "Centro Nacional de Derecho Migratorio",
        },
        url: "https://www.nilc.org/issues/immigration-enforcement/",
        description: {
          en: "Fact sheets on workplace/home raids and enforcement priorities.",
          es: "Hojas informativas sobre redadas laborales/domesticas y prioridades.",
        },
      },
      {
        label: {
          en: "United We Dream – Deportation Defense",
          es: "United We Dream – Defensa contra deportacion",
        },
        url: "https://unitedwedream.org/our-work/deportation-defense/",
        description: {
          en: "Rapid-response guides and hotline details for families facing arrests.",
          es: "Guias de respuesta rapida y lineas de ayuda para familias.",
        },
      },
    ],
  },
  {
    title: { en: "Community Defense & Resistance", es: "Defensa y resistencia comunitaria" },
    links: [
      {
        label: { en: "UndocuBlack Network", es: "UndocuBlack Network" },
        url: "https://undocublack.org/",
        description: {
          en: "Black-led immigrant advocacy with mutual aid, hotline, and detention support info.",
          es: "Defensa migrante liderada por personas negras con apoyo y lineas de ayuda.",
        },
      },
      {
        label: { en: "Freedom for Immigrants", es: "Freedom for Immigrants" },
        url: "https://www.freedomforimmigrants.org/",
        description: {
          en: "National hotline, visitation network, and abuse reporting database.",
          es: "Linea nacional, red de visitas y base de datos de denuncias.",
        },
      },
      {
        label: { en: "Immigrant Defense Project", es: "Immigrant Defense Project" },
        url: "https://www.immigrantdefenseproject.org/resources/",
        description: {
          en: "Guides on ICE-police collaboration, legal defense, and rapid-response preparation.",
          es: "Guias sobre colaboracion ICE-policia, defensa legal y respuesta rapida.",
        },
      },
      {
        label: {
          en: "National Day Laborer Organizing Network",
          es: "Red Nacional de Jornaleros",
        },
        url: "https://ndlon.org/",
        description: {
          en: "Toolkits for workplace/community raids, deportation defense, and protest actions.",
          es: "Herramientas para redadas comunitarias, defensa y accion colectiva.",
        },
      },
      {
        label: { en: "Mijente – No Tech for ICE", es: "Mijente – No Tech for ICE" },
        url: "https://mijente.net/",
        description: {
          en: "Latinx-led organizing hub with research on surveillance and resistance strategies.",
          es: "Organizacion latinx con investigacion sobre vigilancia y resistencia.",
        },
      },
      {
        label: {
          en: "Freedom for Immigrants – ICEwatch (RAICES)",
          es: "Freedom for Immigrants – ICEwatch (RAICES)",
        },
        url: "https://www.raicestexas.org/ice-watch/",
        description: {
          en: "Crowdsourced map of ICE raids, protests, and hotline numbers.",
          es: "Mapa colaborativo de redadas, protestas y lineas de ayuda.",
        },
      },
    ],
  },
  {
    title: { en: "Legal Aid & Specialized Toolkits", es: "Asistencia legal y guias" },
    links: [
      {
        label: {
          en: "Church World Service – Deportation Defense Toolkit",
          es: "Church World Service – Kit de defensa contra deportacion",
        },
        url: "https://cwsglobal.org/our-work/",
        description: {
          en: "Faith/community-service hub; includes deportation defense toolkit and hotline links.",
          es: "Centro de servicios comunitarios con guias y lineas de ayuda.",
        },
      },
      {
        label: { en: "Immigration Equality", es: "Immigration Equality" },
        url: "https://immigrationequality.org/",
        description: {
          en: "Legal help and detention resources for LGBTQ+ and HIV-positive immigrants.",
          es: "Ayuda legal y recursos para personas LGBTQ+ y con VIH.",
        },
      },
      {
        label: {
          en: "National Immigration Project (NIPNLG)",
          es: "Proyecto Nacional de Inmigracion (NIPNLG)",
        },
        url: "https://nipnlg.org/work",
        description: {
          en: "Impact litigation updates, FOIA docs, and practice advisories reporters can cite.",
          es: "Actualizaciones de litigios, FOIA y guias de practica.",
        },
      },
    ],
  },
  {
    title: { en: "Arrest & Removal Ratios", es: "Tasas de arresto y expulsion" },
    links: [
      {
        label: { en: "ICE FY ERO Annual Report", es: "Informe anual ERO (ICE)" },
        url: "https://www.ice.gov/information-library/annual-report",
        description: {
          en: "Breakdowns of arrests/removals with and without prior criminal convictions.",
          es: "Desglose de arrestos/expulsiones con y sin antecedentes.",
        },
      },
      {
        label: { en: "DHS Yearbook – Enforcement Tables 38–41", es: "Anuario DHS – Tablas 38–41" },
        url: "https://www.dhs.gov/immigration-statistics/yearbook",
        description: {
          en: "Detailed removals by field office, nationality, and criminality.",
          es: "Expulsiones detalladas por oficina, nacionalidad y criminalidad.",
        },
      },
    ],
  },
  {
    title: { en: "Media & Investigative Coverage", es: "Cobertura periodistica" },
    links: [
      {
        label: { en: "MeidasTouch", es: "MeidasTouch" },
        url: "https://meidastouch.com/news",
        description: {
          en: "Investigative coverage and interviews about ICE policy impacts.",
          es: "Cobertura investigativa y entrevistas sobre politicas de ICE.",
        },
      },
      {
        label: { en: "Brian Tyler Cohen", es: "Brian Tyler Cohen" },
        url: "https://briantylercohen.com/",
        description: {
          en: "Video explainers and newsletters on immigration enforcement developments.",
          es: "Videos explicativos y boletines sobre control migratorio.",
        },
      },
      {
        label: { en: "DocumentedNY", es: "DocumentedNY" },
        url: "https://documentedny.com/",
        description: {
          en: "Local NYC investigations into ICE raids, detention trends, and lawsuits.",
          es: "Investigaciones locales en NYC sobre redadas y tendencias.",
        },
      },
      {
        label: {
          en: "The Economist – Trump Approval Tracker",
          es: "The Economist – Trump Approval Tracker",
        },
        url: "https://www.economist.com/interactive/trump-approval-tracker",
        description: {
          en: "Interactive tracker of Trump approval ratings.",
          es: "Seguimiento interactivo de aprobacion de Trump.",
        },
      },
      {
        label: { en: "The Marshall Project", es: "The Marshall Project" },
        url: "https://www.themarshallproject.org/",
        description: {
          en: "National reporting on detention conditions and due process failures.",
          es: "Reportajes nacionales sobre condiciones de detencion y debido proceso.",
        },
      },
    ],
  },
  {
    title: { en: "Courts & Legal Filings", es: "Tribunales y documentos legales" },
    links: [
      {
        label: { en: "CourtListener", es: "CourtListener" },
        url: "https://www.courtlistener.com/",
        description: {
          en: "Free access to federal dockets, opinions, and briefs involving ICE.",
          es: "Acceso gratuito a expedientes y documentos federales sobre ICE.",
        },
      },
      {
        label: {
          en: "American Immigration Council – Litigation",
          es: "American Immigration Council – Litigios",
        },
        url: "https://www.americanimmigrationcouncil.org/litigation",
        description: {
          en: "Impact litigation tracking constitutional and statutory challenges nationwide.",
          es: "Seguimiento de litigios de impacto a nivel nacional.",
        },
      },
    ],
  },
];

const resolveUrl = (url: string, language: Language): string => {
  if (!url.startsWith("/")) {
    return url;
  }
  if (language === "es") {
    return `/es${url}`;
  }
  return url;
};

export const getResourceSections = (language: Language): ResourceSection[] =>
  RESOURCE_SECTIONS_DATA.map((section) => ({
    title: section.title[language],
    links: section.links.map((link) => ({
      label: link.label[language],
      url: resolveUrl(link.url, language),
      description: link.description ? link.description[language] : undefined,
    })),
  }));
