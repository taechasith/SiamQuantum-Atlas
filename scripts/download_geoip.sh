#!/usr/bin/env bash
# Download MaxMind GeoLite2-City.mmdb
# Requires MAXMIND_LICENSE_KEY env var
# Usage: bash scripts/download_geoip.sh

set -euo pipefail

LICENSE_KEY="${MAXMIND_LICENSE_KEY:?Set MAXMIND_LICENSE_KEY in .env}"
DEST="data/geoip/GeoLite2-City.mmdb"
URL="https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=${LICENSE_KEY}&suffix=tar.gz"

mkdir -p data/geoip
curl -fsSL "$URL" -o /tmp/geolite2.tar.gz
tar -xzf /tmp/geolite2.tar.gz -C /tmp
find /tmp -name "GeoLite2-City.mmdb" -exec mv {} "$DEST" \;
echo "GeoLite2-City.mmdb installed to $DEST"
