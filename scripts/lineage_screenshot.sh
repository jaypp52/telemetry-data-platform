#!/usr/bin/env bash
# Render the dbt docs lineage graph to docs/lineage.png with headless Chrome.
# Needs dbt/target/static_index.html (`make dbt-docs`) and google-chrome on the host.
set -euo pipefail
cd "$(dirname "$0")/.."
page="$PWD/dbt/target/static_index.html"
[ -f "$page" ] || { echo "run 'make dbt-docs' first" >&2; exit 1; }
chrome="${CHROME:-$(command -v google-chrome || command -v chromium || command -v chromium-browser)}"
"$chrome" --headless=new --disable-gpu --hide-scrollbars --window-size=1800,1000 \
  --virtual-time-budget=15000 --screenshot="$PWD/docs/lineage.png" "file://$page#!/overview?g_v=1"
echo "wrote docs/lineage.png"
