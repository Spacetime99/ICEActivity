import type { Language } from "./i18n";

type LocalizedString = {
  en: string;
  es: string;
};

type OverlaySection = {
  title: LocalizedString;
  paragraphs?: LocalizedString[];
  listItems?: LocalizedString[];
  note?: LocalizedString;
};

export type OverlayContent = {
  title: LocalizedString;
  subtitle?: LocalizedString;
  sections: OverlaySection[];
  footer?: LocalizedString;
};

export const ABOUT_CONTENT: OverlayContent = {
  title: { en: "About", es: "Sobre" },
  subtitle: {
    en: "ICE Incident News Map — about the project and its goals.",
    es: "ICE Incident News Map — sobre el proyecto y sus objetivos.",
  },
  sections: [
    {
      title: { en: "Purpose", es: "Proposito" },
      paragraphs: [
        {
          en: "ICEMap (ICE Incident News Map) aggregates publicly reported coverage of immigration enforcement activity so communities, advocates, and researchers can quickly understand where incidents are being reported and how coverage changes over time.",
          es: "ICEMap (ICE Incident News Map) agrega cobertura publicada sobre actividad de control migratorio para que comunidades, defensores e investigadores entiendan donde se reportan incidentes y como cambia la cobertura con el tiempo.",
        },
      ],
    },
    {
      title: { en: "What the map shows", es: "Que muestra el mapa" },
      listItems: [
        {
          en: "Short summaries of news reports and public updates.",
          es: "Resumenes breves de reportes y actualizaciones publicas.",
        },
        {
          en: "Approximate locations derived from place names in the report text.",
          es: "Ubicaciones aproximadas derivadas de nombres de lugares en el texto.",
        },
        {
          en: "Time filters that let you focus on recent coverage.",
          es: "Filtros de tiempo para enfocarse en cobertura reciente.",
        },
      ],
    },
    {
      title: { en: "Note", es: "Aviso" },
      note: {
        en: "We do not publish addresses of individuals or private residences, and we do not encourage harassment or interference with any person. The map is an information resource built from publicly available reporting.",
        es: "No publicamos direcciones de personas ni residencias privadas y no fomentamos acoso ni interferencia. El mapa es un recurso informativo basado en fuentes publicas.",
      },
    },
    {
      title: { en: "Sources", es: "Fuentes" },
      paragraphs: [
        {
          en: "Summaries are drawn from publisher-provided descriptions (for example RSS feed summaries) and converted into brief text entries that link back to the original reporting.",
          es: "Los resumenes provienen de descripciones publicadas por los medios (por ejemplo en RSS) y se muestran como entradas breves con enlace al reporte original.",
        },
      ],
    },
  ],
  footer: {
    en: "Questions or corrections? Use the feedback form.\nThis application was written entirely by AI.",
    es: "Preguntas o correcciones? Usa el formulario de comentarios.\nEsta aplicacion fue escrita completamente por IA.",
  },
};

export const METHODOLOGY_CONTENT: OverlayContent = {
  title: { en: "Methodology", es: "Metodologia" },
  subtitle: {
    en: "ICE Incident News Map — how reports are collected, summarized, and mapped.",
    es: "ICE Incident News Map — como se recopilan, resumen y geolocalizan los reportes.",
  },
  sections: [
    {
      title: { en: "Collection", es: "Recopilacion" },
      listItems: [
        {
          en: "We ingest public RSS feeds and news sources on a regular schedule.",
          es: "Ingerimos fuentes RSS y medios publicos de forma regular.",
        },
        {
          en: "We extract headlines, summaries, and publication timestamps.",
          es: "Extraemos titulares, resumenes y fechas de publicacion.",
        },
        {
          en: "We look for place names or facility mentions in each report.",
          es: "Buscamos nombres de lugares o instalaciones en cada reporte.",
        },
      ],
    },
    {
      title: { en: "Mapping", es: "Mapa" },
      listItems: [
        {
          en: "Locations are resolved to approximate coordinates via a lookup table.",
          es: "Las ubicaciones se resuelven a coordenadas aproximadas con una tabla base.",
        },
        {
          en: "When a place is ambiguous, we fall back to a general location marker.",
          es: "Cuando un lugar es ambiguo, usamos un marcador general.",
        },
        {
          en: "Points within a small radius are grouped to reduce visual clutter.",
          es: "Los puntos cercanos se agrupan para reducir el ruido visual.",
        },
      ],
    },
    {
      title: { en: "Time windows", es: "Ventanas de tiempo" },
      paragraphs: [
        {
          en: "The map offers predefined windows (3 days, 7 days, 1 month, 3 months) and an all-time view. Filters are based on the reported publication time.",
          es: "El mapa ofrece ventanas predefinidas (3 dias, 7 dias, 1 mes, 3 meses) y vista completa. Los filtros se basan en la fecha de publicacion.",
        },
      ],
    },
    {
      title: { en: "Note", es: "Aviso" },
      note: {
        en: "This project maps media reporting, not verified on-the-ground confirmation. Always cross-check with original sources.",
        es: "Este proyecto mapea cobertura periodistica, no confirmacion directa en terreno. Verifique siempre con las fuentes originales.",
      },
    },
  ],
};

export const getOverlayText = (value: LocalizedString, language: Language) => value[language];
