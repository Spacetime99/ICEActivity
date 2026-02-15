import type { ReactNode } from "react";
import type { Language } from "./i18n";
import FatalitiesBanner from "./FatalitiesBanner";

type PageHeaderProps = {
  headerClassName: string;
  brandClassName: string;
  textClassName?: string;
  title: string;
  subtitle?: string | null;
  appName: string;
  updatedLabel?: string | null;
  nav: ReactNode;
  languageLabel: string;
  language: Language;
  languageOptions: Record<string, string>;
  onLanguageChange: (language: Language) => void;
  selectId: string;
  iconSrc: string;
  iconAlt: string;
  children?: ReactNode;
};

const PageHeader = ({
  headerClassName,
  brandClassName,
  textClassName,
  title,
  subtitle,
  appName,
  updatedLabel,
  nav,
  languageLabel,
  language,
  languageOptions,
  onLanguageChange,
  selectId,
  iconSrc,
  iconAlt,
  children,
}: PageHeaderProps) => (
  <header className={headerClassName}>
    <div className={brandClassName}>
      <img className="app-icon" src={iconSrc} alt={iconAlt} />
      <div className={textClassName}>
        <div className="eyebrow-row">
          <p className="eyebrow">{appName}</p>
          {updatedLabel && <span className="data-updated-inline">{updatedLabel}</span>}
        </div>
        <h1>{title}</h1>
        {subtitle ? <p className="hero-subtitle">{subtitle}</p> : null}
      </div>
    </div>
    <div className="header-actions">
      {nav}
      <div className="header-right">
        <FatalitiesBanner />
        <div className="language-select">
          <label htmlFor={selectId}>{languageLabel}</label>
          <select
            id={selectId}
            value={language}
            onChange={(event) => onLanguageChange(event.target.value as Language)}
          >
            {Object.entries(languageOptions).map(([code, label]) => (
              <option key={code} value={code}>
                {label}
              </option>
            ))}
          </select>
        </div>
      </div>
    </div>
    {children}
  </header>
);

export default PageHeader;
