#!/usr/bin/env bash
set -euo pipefail
USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
urls=(
  "https://www.ice.gov/statistics"
  "https://www.dhs.gov/immigration-statistics/yearbook"
  "https://www.cbp.gov/newsroom/stats/southwest-land-border-encounters"
  "https://www.justice.gov/eoir/workload-and-adjudication-statistics"
  "https://trac.syr.edu/immigration/"
  "https://www.ice.gov/detain/detention-facilities"
  "https://locator.ice.gov/odls/#/index"
  "https://www.ice.gov/contact/opla"
  "https://www.aclu.org/know-your-rights/immigrants-rights"
  "https://www.ilrc.org/community-resources"
  "https://www.nilc.org/issues/immigration-enforcement/"
  "https://unitedwedream.org/our-work/deportation-defense/"
  "https://www.ice.gov/doclib/eoy/iceAnnualReportFY2024.pdf"
  "https://ohss.dhs.gov/topics/immigration/yearbook"
  "https://www.meidastouch.com"
  "https://briantylercohen.substack.com/"
  "https://documentedny.com/"
  "https://www.themarshallproject.org/"
  "https://www.courtlistener.com/"
  "https://www.americanimmigrationcouncil.org/impact/litigation"
  "https://undocublack.org/"
  "https://www.freedomforimmigrants.org/"
  "https://www.immigrantdefenseproject.org/resources/"
  "https://ndlon.org/"
  "https://mijente.net/"
  "https://www.raicestexas.org/ice-watch/"
  "https://cwsglobal.org/our-work/"
  "https://immigrationequality.org/"
  "https://nipnlg.org/work"
)
for url in "${urls[@]}"; do
  echo "Checking $url"
  status_head=$(curl -A "$USER_AGENT" -I -sS --max-time 20 "$url" | head -n 1 || echo "HEAD_FAILED")
  echo "$status_head"
  body_file=$(mktemp)
  http_code=$(curl -A "$USER_AGENT" -sS -w "%{http_code}" -o "$body_file" -L --max-time 25 "$url" || echo "000")
  if [ "$http_code" = "200" ] || [ "$http_code" = "301" ] || [ "$http_code" = "302" ]; then
    echo "GET OK ($http_code)"
  else
    echo "GET failed ($http_code)"
  fi
  if [ "$http_code" = "404" ] || { [ "$http_code" != "000" ] && [ "$http_code" -ge 400 ]; }; then
    echo "Content indicates error"
  fi
  rm -f "$body_file"
  echo "-----"
done
