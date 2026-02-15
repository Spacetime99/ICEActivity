export type ChartItem = {
  title: string;
  href: string;
  imgSrc?: string;
  imgAlt?: string;
  creditText?: string;
  creditHref?: string;
  creditLabel?: string;
  embedUrl?: string;
  linkOnly?: boolean;
  layout?: "wide";
};

export const CHARTS: ChartItem[] = [
  {
    title: "Infographic: Number of ICE Detainees Surged 65% in 2025 | Statista",
    href: "https://www.statista.com/chart/35665/number-of-ice-detainees/",
    imgSrc: "https://cdn.statcdn.com/Infographic/images/normal/35665.jpeg",
    imgAlt: "Infographic: Number of ICE Detainees Surged 65% in 2025 | Statista",
    creditText: "You will find more infographics at",
    creditHref: "https://www.statista.com/chartoftheday/",
    creditLabel: "Statista",
  },
  {
    title: "Infographic: Woman Killed in Minneapolis Not the First ICE Shooting | Statista",
    href: "https://www.statista.com/chart/35648/civilians-killed-wounded-shot-at-by-immigration-officers-in-the-united-states/",
    imgSrc: "https://cdn.statcdn.com/Infographic/images/normal/35648.jpeg",
    imgAlt: "Infographic: Woman Killed in Minneapolis Not the First ICE Shooting | Statista",
    creditText: "You will find more infographics at",
    creditHref: "https://www.statista.com/chartoftheday/",
    creditLabel: "Statista",
  },
  {
    title:
      "Statistic: Resident population of the United States in July 2000 and July 2024, by race (in millions) | Statista",
    href: "https://www.statista.com/statistics/183489/population-of-the-us-by-ethnicity-since-2000/",
    imgSrc: "https://www.statista.com/graphic/1/183489/population-of-the-us-by-ethnicity-since-2000.jpg",
    imgAlt:
      "Statistic: Resident population of the United States in July 2000 and July 2024, by race (in millions) | Statista",
    creditText: "Find more statistics at",
    creditHref: "https://www.statista.com",
    creditLabel: "Statista",
  },
  {
    title: "Infographic: Has ICE Been More Active Under Trump? | Statista",
    href: "https://www.statista.com/chart/34071/number-of-monthly-arrests-made-by-ice/",
    imgSrc: "https://cdn.statcdn.com/Infographic/images/normal/34071.jpeg",
    imgAlt: "Infographic: Has ICE Been More Active Under Trump? | Statista",
    creditText: "You will find more infographics at",
    creditHref: "https://www.statista.com/chartoftheday/",
    creditLabel: "Statista",
  },
  {
    title: "TRAC - Immigration Detention Quick Facts",
    href: "https://tracreports.org/immigration/quickfacts/",
    embedUrl: "https://tracreports.org/immigration/quickfacts/",
    layout: "wide",
  },
  {
    title: "TRAC - All ICE Removals",
    href: "https://tracreports.org/phptools/immigration/remove/about_data.html",
    embedUrl: "https://tracreports.org/phptools/immigration/remove/about_data.html",
  },
  {
    title: "Vera - ICE Detention Trends",
    href: "https://www.vera.org/ice-detention-trends",
    embedUrl: "https://www.vera.org/ice-detention-trends",
  },
  {
    title: "OpenICE - detention facility map + population estimates",
    href: "https://www.openice.org/",
    embedUrl: "https://www.openice.org/",
  },
  {
    title: "Immigration Enforcement Dashboard",
    href: "https://enforcementdashboard.com/",
    embedUrl: "https://enforcementdashboard.com/",
  },
  {
    title: "Fatal Encounters - Visualizations",
    href: "https://fatalencounters.org/our-visualizations/",
    embedUrl: "https://fatalencounters.org/our-visualizations/",
    layout: "wide",
  },
  {
    title: "CBP - Southwest Land Border Encounters",
    href: "https://www.cbp.gov/newsroom/stats/southwest-land-border-encounters",
    linkOnly: true,
  },
  {
    title: "CBP - Stats & Summaries hub",
    href: "https://www.cbp.gov/newsroom/stats",
    linkOnly: true,
  },
  {
    title: "CBP - Public Data Portal",
    href: "https://www.cbp.gov/newsroom/stats/cbp-public-data-portal",
    linkOnly: true,
  },
  {
    title: "ICE - ERO Statistics dashboards",
    href: "https://www.ice.gov/statistics",
    linkOnly: true,
  },
];
