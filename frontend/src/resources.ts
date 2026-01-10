export type ResourceLink = {
  label: string;
  url: string;
  description?: string;
};

export type ResourceSection = {
  title: string;
  links: ResourceLink[];
};

export const RESOURCE_SECTIONS: ResourceSection[] = [
  {
    title: "Official Data & Dashboards",
    links: [
      {
        label: "ICE Data Portal",
        url: "https://www.ice.gov/statistics",
        description: "Monthly CSVs for book-ins, detained population, removals, and ATD stats.",
      },
      {
        label: "DHS Yearbook of Immigration Statistics",
        url: "https://www.dhs.gov/immigration-statistics/yearbook",
        description: "Annual reference tables covering arrests, removals, and enforcement outcomes.",
      },
      {
        label: "CBP Southwest Land Border Encounters",
        url: "https://www.cbp.gov/newsroom/stats/southwest-land-border-encounters",
        description: "Monthly encounter counts by sector, family unit category, and nationality.",
      },
      {
        label: "EOIR Adjudication Statistics",
        url: "https://www.justice.gov/eoir/workload-and-adjudication-statistics",
        description: "Immigration court filings, completions, and decision outcomes.",
      },
      {
        label: "TRAC Immigration Dashboards",
        url: "https://trac.syr.edu/immigration/",
        description: "Independent detention, deportation, and court data (download CSVs per chart).",
      },
    ],
  },
  {
    title: "Facilities & Detainee Lookup",
    links: [
      {
        label: "ICE Detention Facilities Directory",
        url: "https://www.ice.gov/detain/detention-facilities",
        description: "Official facility addresses, field office contacts, and phone numbers.",
      },
      {
        label: "ICE Detainee Locator",
        url: "https://locator.ice.gov/odls/#/index",
        description: "Search for individuals currently in ICE custody.",
      },
      {
        label: "OPLA Field Office Contacts",
        url: "https://www.ice.gov/contact/opla",
        description: "Regional Office of the Principal Legal Advisor contact information.",
      },
    ],
  },
  {
    title: "Know Your Rights & Community Guides",
    links: [
      {
        label: "ACLU – Immigrants' Rights",
        url: "https://www.aclu.org/know-your-rights/immigrants-rights",
        description: "Steps to take if approached by ICE, including right-to-remain-silent guidance.",
      },
      {
        label: "Immigrant Legal Resource Center",
        url: "https://www.ilrc.org/community-resources",
        description: "Community tip sheets on ICE encounters and raid preparedness.",
      },
      {
        label: "National Immigration Law Center",
        url: "https://www.nilc.org/issues/immigration-enforcement/",
        description: "Fact sheets on workplace/home raids and enforcement priorities.",
      },
      {
        label: "United We Dream – Deportation Defense",
        url: "https://unitedwedream.org/our-work/deportation-defense/",
        description: "Rapid-response guides and hotline details for families facing arrests.",
      },
    ],
  },
  {
    title: "Community Defense & Resistance",
    links: [
      {
        label: "UndocuBlack Network",
        url: "https://undocublack.org/",
        description: "Black-led immigrant advocacy with mutual aid, hotline, and detention support info.",
      },
      {
        label: "Freedom for Immigrants",
        url: "https://www.freedomforimmigrants.org/",
        description: "National hotline, visitation network, and abuse reporting database.",
      },
      {
        label: "Immigrant Defense Project",
        url: "https://www.immigrantdefenseproject.org/resources/",
        description: "Guides on ICE-police collaboration, legal defense, and rapid-response preparation.",
      },
      {
        label: "National Day Laborer Organizing Network",
        url: "https://ndlon.org/",
        description: "Toolkits for workplace/community raids, deportation defense, and protest actions.",
      },
      {
        label: "Mijente – No Tech for ICE",
        url: "https://mijente.net/",
        description: "Latinx-led organizing hub with research on surveillance and resistance strategies.",
      },
      {
        label: "Freedom for Immigrants – ICEwatch (RAICES)",
        url: "https://www.raicestexas.org/ice-watch/",
        description: "Crowdsourced map of ICE raids, protests, and hotline numbers.",
      },
    ],
  },
  {
    title: "Legal Aid & Specialized Toolkits",
    links: [
      {
        label: "Church World Service – Deportation Defense Toolkit",
        url: "https://cwsglobal.org/our-work/",
        description: "Faith/community-service hub; includes deportation defense toolkit and hotline links.",
      },
      {
        label: "Immigration Equality",
        url: "https://immigrationequality.org/",
        description: "Legal help and detention resources for LGBTQ+ and HIV-positive immigrants.",
      },
      {
        label: "National Immigration Project (NIPNLG)",
        url: "https://nipnlg.org/work",
        description: "Impact litigation updates, FOIA docs, and practice advisories reporters can cite.",
      },
    ],
  },
  {
    title: "Arrest & Removal Ratios",
    links: [
      {
        label: "ICE FY ERO Annual Report",
        url: "https://www.ice.gov/doclib/eoy/iceAnnualReportFY2024.pdf",
        description: "Breakdowns of arrests/removals with and without prior criminal convictions.",
      },
      {
        label: "DHS Yearbook – Enforcement Tables 38–41",
        url: "https://ohss.dhs.gov/topics/immigration/yearbook",
        description: "Detailed removals by field office, nationality, and criminality.",
      },
    ],
  },
  {
    title: "Media & Investigative Coverage",
    links: [
      {
        label: "MeidasTouch",
        url: "https://www.meidastouch.com",
        description: "Investigative coverage and interviews about ICE policy impacts.",
      },
      {
        label: "Brian Tyler Cohen",
        url: "https://briantylercohen.substack.com/",
        description: "Video explainers and newsletters on immigration enforcement developments.",
      },
      {
        label: "DocumentedNY",
        url: "https://documentedny.com/",
        description: "Local NYC investigations into ICE raids, detention trends, and lawsuits.",
      },
      {
        label: "The Marshall Project",
        url: "https://www.themarshallproject.org/",
        description: "National reporting on detention conditions and due process failures.",
      },
    ],
  },
  {
    title: "Courts & Legal Filings",
    links: [
      {
        label: "CourtListener",
        url: "https://www.courtlistener.com/",
        description: "Free access to federal dockets, opinions, and briefs involving ICE.",
      },
      {
        label: "American Immigration Council – Litigation",
        url: "https://www.americanimmigrationcouncil.org/impact/litigation",
        description: "Impact litigation tracking constitutional and statutory challenges nationwide.",
      },
    ],
  },
];
