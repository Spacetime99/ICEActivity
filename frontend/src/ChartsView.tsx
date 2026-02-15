import { CHARTS, type ChartItem } from "./charts";
import { trackOutboundClick } from "./analytics";

type ChartsViewProps = {
  analyticsPage?: string;
};

const ChartsView = ({ analyticsPage = "charts" }: ChartsViewProps) => {
  const handleOutboundClick =
    (label: string, url?: string | null, context?: string) => () => {
      if (!url || !analyticsPage) {
        return;
      }
      trackOutboundClick(label, url, analyticsPage, context);
    };

  const gridCharts = CHARTS.filter(
    (chart) => !chart.linkOnly && chart.layout !== "wide",
  );
  const wideCharts = CHARTS.filter(
    (chart) => !chart.linkOnly && chart.layout === "wide",
  );
  const linkCharts = CHARTS.filter((chart) => chart.linkOnly);

  const renderChartCard = (chart: ChartItem) => (
    <section key={chart.href} className="resource-card chart-card">
      <h2>{chart.title}</h2>
      {chart.imgSrc ? (
        <a
          href={chart.href}
          target="_blank"
          rel="noreferrer"
          onClick={handleOutboundClick(chart.title, chart.href, "charts")}
        >
          <img src={chart.imgSrc} alt={chart.imgAlt ?? chart.title} />
        </a>
      ) : (
        <div className="chart-embed">
          {chart.embedUrl && (
            <iframe title={chart.title} src={chart.embedUrl} loading="lazy" />
          )}
          <a
            className="chart-embed-link"
            href={chart.href}
            target="_blank"
            rel="noreferrer"
            onClick={handleOutboundClick(chart.title, chart.href, "charts")}
          >
            Open chart
          </a>
        </div>
      )}
      {chart.creditText && chart.creditHref && chart.creditLabel && (
        <p className="chart-credit">
          {chart.creditText}{" "}
          <a
            href={chart.creditHref}
            target="_blank"
            rel="noreferrer"
            onClick={handleOutboundClick(
              chart.creditLabel,
              chart.creditHref,
              "charts",
            )}
          >
            {chart.creditLabel}
          </a>
          .
        </p>
      )}
    </section>
  );

  return (
    <div className="charts-view-wrapper">
      <div className="resources-view charts-view">
        {gridCharts.map((chart) => renderChartCard(chart))}
      </div>
      {wideCharts.length > 0 && (
        <div className="charts-wide">
          {wideCharts.map((chart) => renderChartCard(chart))}
        </div>
      )}
      {linkCharts.length > 0 && (
        <div className="chart-links">
          {linkCharts.map((chart) => (
            <div className="chart-link-row" key={chart.href}>
              <span className="chart-link-text">{chart.title}</span>
              <a
                className="chart-link-action"
                href={chart.href}
                target="_blank"
                rel="noreferrer"
                onClick={handleOutboundClick(chart.title, chart.href, "charts")}
              >
                Open chart
              </a>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default ChartsView;
