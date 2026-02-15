import { useEffect, useState } from "react";
import { STATIC_DATA_BASE_URL } from "./config";

const DEATHS_URL = `${import.meta.env.BASE_URL ?? "/"}deaths.html`;

type IndexPayload = {
  counts?: {
    context?: Record<string, number | string | null | undefined>;
  };
};

const FatalitiesBanner = () => {
  const [count, setCount] = useState<number | null>(null);

  useEffect(() => {
    const abort = new AbortController();
    const run = async () => {
      try {
        const response = await fetch(`${STATIC_DATA_BASE_URL}/index.json`, {
          signal: abort.signal,
        });
        if (response.ok) {
          const payload = (await response.json()) as IndexPayload;
          const contextCounts = payload.counts?.context ?? {};
          const total = Object.values(contextCounts).reduce((sum, value) => {
            const parsed =
              typeof value === "number"
                ? value
                : typeof value === "string"
                  ? Number(value)
                  : 0;
            return sum + (Number.isFinite(parsed) ? parsed : 0);
          }, 0);
          if (total > 0) {
            setCount(total);
            return;
          }
        }
      } catch {
        // fallback below
      }

      try {
        const fallback = await fetch(`${STATIC_DATA_BASE_URL}/deaths.json`, {
          signal: abort.signal,
        });
        if (!fallback.ok) {
          return;
        }
        const rows = (await fallback.json()) as unknown;
        if (Array.isArray(rows)) {
          setCount(rows.length);
        }
      } catch {
        // Keep the banner visible even if data fetches fail.
      }
    };
    void run();
    return () => abort.abort();
  }, []);

  const label = count ?? "—";
  return (
    <p className="fatalities-banner">
      <a href={DEATHS_URL}>Fatalities at the hands of ICE: {label}</a>
    </p>
  );
};

export default FatalitiesBanner;
