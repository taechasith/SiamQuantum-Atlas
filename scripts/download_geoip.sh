#!/usr/bin/env bash
# Download MaxMind GeoLite2-City.mmdb to data/geoip/
# Reads MAXMIND_LICENSE_KEY from .env in the repo root.
# Idempotent: skips re-download if .mmdb is <30 days old.
set -euo pipefail

DEST="data/geoip/GeoLite2-City.mmdb"
MAX_AGE_DAYS=30
DOTENV=".env"
BASE_URL="https://download.maxmind.com/app/geoip_download"
TMP_DIR=".cache/geoip_dl"
TMP_TAR="$TMP_DIR/GeoLite2-City.tar.gz"
TMP_SHA="$TMP_DIR/GeoLite2-City.tar.gz.sha256"

# ── idempotency ──────────────────────────────────────────────────────────────
if [ -f "$DEST" ]; then
    AGE_DAYS=$(python -c "
import os, time
age = (time.time() - os.path.getmtime('$DEST')) / 86400
print(int(age))
")
    if [ "$AGE_DAYS" -lt "$MAX_AGE_DAYS" ]; then
        echo "GeoLite2-City.mmdb is ${AGE_DAYS} days old (<${MAX_AGE_DAYS}). Skipping."
        ls -lh "$DEST"
        exit 0
    fi
    echo "GeoLite2-City.mmdb is ${AGE_DAYS} days old — refreshing."
fi

# ── read license key from .env ───────────────────────────────────────────────
if [ ! -f "$DOTENV" ]; then
    echo "ERROR: $DOTENV not found. Run from repo root." >&2; exit 1
fi
LICENSE_KEY=$(grep "^MAXMIND_LICENSE_KEY=" "$DOTENV" | cut -d'=' -f2- | tr -d '\r')
if [ -z "$LICENSE_KEY" ]; then
    echo "ERROR: MAXMIND_LICENSE_KEY not set in $DOTENV" >&2; exit 1
fi

# ── download ─────────────────────────────────────────────────────────────────
mkdir -p "$TMP_DIR"
echo "Downloading GeoLite2-City tarball..."
curl -fsSL \
    "${BASE_URL}?edition_id=GeoLite2-City&license_key=${LICENSE_KEY}&suffix=tar.gz" \
    -o "$TMP_TAR"

echo "Downloading SHA256 sidecar..."
curl -fsSL \
    "${BASE_URL}?edition_id=GeoLite2-City&license_key=${LICENSE_KEY}&suffix=tar.gz.sha256" \
    -o "$TMP_SHA"

# ── verify checksum ──────────────────────────────────────────────────────────
echo "Verifying SHA256..."
EXPECTED=$(awk '{print $1}' "$TMP_SHA")
ACTUAL=$(python -c "
import hashlib
h = hashlib.sha256()
with open(r'$TMP_TAR', 'rb') as f:
    for chunk in iter(lambda: f.read(65536), b''):
        h.update(chunk)
print(h.hexdigest())
")
if [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "ERROR: SHA256 mismatch!" >&2
    echo "  expected: $EXPECTED" >&2
    echo "  actual:   $ACTUAL" >&2
    rm -rf "$TMP_DIR"
    exit 1
fi
echo "Checksum OK."

# ── extract ──────────────────────────────────────────────────────────────────
echo "Extracting .mmdb..."
mkdir -p data/geoip
# Strip the date-versioned parent dir (GeoLite2-City_YYYYMMDD/)
tar -xzf "$TMP_TAR" -C "$TMP_DIR" --wildcards "*/GeoLite2-City.mmdb"
find "$TMP_DIR" -name "GeoLite2-City.mmdb" -exec mv {} "$DEST" \;

# ── cleanup ──────────────────────────────────────────────────────────────────
rm -rf "$TMP_DIR"

echo "Done."
ls -lh "$DEST"
