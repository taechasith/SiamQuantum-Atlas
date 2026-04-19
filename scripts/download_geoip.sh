#!/usr/bin/env bash
# Download MaxMind GeoLite2-City.mmdb and GeoLite2-ASN.mmdb to data/geoip/
# Reads MAXMIND_LICENSE_KEY from .env in the repo root.
# Idempotent: skips re-download if .mmdb is <30 days old.
set -euo pipefail

MAX_AGE_DAYS=30
DOTENV=".env"
BASE_URL="https://download.maxmind.com/app/geoip_download"
TMP_DIR=".cache/geoip_dl"

# ── read license key from .env ───────────────────────────────────────────────
if [ ! -f "$DOTENV" ]; then
    echo "ERROR: $DOTENV not found. Run from repo root." >&2; exit 1
fi
LICENSE_KEY=$(grep "^MAXMIND_LICENSE_KEY=" "$DOTENV" | cut -d'=' -f2- | tr -d '\r')
if [ -z "$LICENSE_KEY" ]; then
    echo "ERROR: MAXMIND_LICENSE_KEY not set in $DOTENV" >&2; exit 1
fi

mkdir -p data/geoip "$TMP_DIR"

# ── helper: download one edition ─────────────────────────────────────────────
download_edition() {
    local EDITION="$1"           # e.g. GeoLite2-City
    local DEST="data/geoip/${EDITION}.mmdb"
    local TMP_TAR="$TMP_DIR/${EDITION}.tar.gz"
    local TMP_SHA="$TMP_DIR/${EDITION}.tar.gz.sha256"

    if [ -f "$DEST" ]; then
        local AGE_DAYS
        AGE_DAYS=$(python -c "
import os, time
age = (time.time() - os.path.getmtime('$DEST')) / 86400
print(int(age))
")
        if [ "$AGE_DAYS" -lt "$MAX_AGE_DAYS" ]; then
            echo "${EDITION}.mmdb is ${AGE_DAYS} days old (<${MAX_AGE_DAYS}). Skipping."
            ls -lh "$DEST"
            return 0
        fi
        echo "${EDITION}.mmdb is ${AGE_DAYS} days old — refreshing."
    fi

    echo "Downloading ${EDITION} tarball..."
    curl -fsSL \
        "${BASE_URL}?edition_id=${EDITION}&license_key=${LICENSE_KEY}&suffix=tar.gz" \
        -o "$TMP_TAR"

    echo "Downloading ${EDITION} SHA256 sidecar..."
    curl -fsSL \
        "${BASE_URL}?edition_id=${EDITION}&license_key=${LICENSE_KEY}&suffix=tar.gz.sha256" \
        -o "$TMP_SHA"

    echo "Verifying SHA256 for ${EDITION}..."
    local EXPECTED ACTUAL
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
        echo "ERROR: SHA256 mismatch for ${EDITION}!" >&2
        echo "  expected: $EXPECTED" >&2
        echo "  actual:   $ACTUAL" >&2
        rm -f "$TMP_TAR" "$TMP_SHA"
        return 1
    fi
    echo "Checksum OK."

    echo "Extracting ${EDITION}.mmdb..."
    tar -xzf "$TMP_TAR" -C "$TMP_DIR" --wildcards "*/${EDITION}.mmdb"
    find "$TMP_DIR" -name "${EDITION}.mmdb" -exec mv {} "$DEST" \;
    rm -f "$TMP_TAR" "$TMP_SHA"

    echo "Done: $(ls -lh "$DEST" | awk '{print $5, $9}')"
}

# ── download both editions ────────────────────────────────────────────────────
download_edition "GeoLite2-City"
download_edition "GeoLite2-ASN"

# ── cleanup tmp ──────────────────────────────────────────────────────────────
rm -rf "$TMP_DIR"
echo "All GeoIP databases up to date."
