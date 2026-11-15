import type { GeoItem } from '@/api/queries'

export const DOMAIN_LABELS: Record<string, string> = {
  quantum_computing: 'Quantum Computing',
  quantum_fundamentals: 'Quantum Fundamentals',
  quantum_education: 'Quantum Education',
  quantum_policy_industry: 'Policy & Industry',
  quantum_communication: 'Communication & Security',
  quantum_materials: 'Materials & Sensing',
  quantum_sensing: 'Quantum Sensing',
}

export const DOMAIN_COLORS = [
  '#ffffff',
  '#67e8f9',
  '#329dd2',
  '#a5b4fc',
  '#f0abfc',
  '#fbbf24',
  '#34d399',
]

export const DOMAIN_ORDER = [
  'quantum_computing',
  'quantum_fundamentals',
  'quantum_education',
  'quantum_policy_industry',
  'quantum_communication',
  'quantum_materials',
  'quantum_sensing',
]

export const PLATFORM_LABELS: Record<string, string> = {
  youtube: 'YouTube',
  gdelt: 'GDELT',
  manual_seed: 'Manual Seed',
  web: 'Web',
}

const COUNTRY_LABELS: Record<string, string> = {
  TH: 'Thailand',
  US: 'United States',
  GB: 'United Kingdom',
  CN: 'China',
  JP: 'Japan',
  SG: 'Singapore',
  DE: 'Germany',
  KR: 'South Korea',
  AU: 'Australia',
  IN: 'India',
  FR: 'France',
  CA: 'Canada',
  NL: 'Netherlands',
  CH: 'Switzerland',
  SE: 'Sweden',
  IL: 'Israel',
}

const KNOWN_PLACES = [
  { name: 'Bangkok', lat: 13.7563, lng: 100.5018 },
  { name: 'Nonthaburi', lat: 13.8591, lng: 100.5217 },
  { name: 'Pathum Thani', lat: 14.0208, lng: 100.525 },
  { name: 'Samut Prakan', lat: 13.5991, lng: 100.5998 },
  { name: 'Ayutthaya', lat: 14.3532, lng: 100.5689 },
  { name: 'Saraburi', lat: 14.5289, lng: 100.9108 },
  { name: 'Nakhon Ratchasima', lat: 14.9799, lng: 102.0977 },
  { name: 'Khon Kaen', lat: 16.4322, lng: 102.8236 },
  { name: 'Chiang Mai', lat: 18.7883, lng: 98.9853 },
  { name: 'Chiang Rai', lat: 19.9105, lng: 99.8406 },
  { name: 'Phitsanulok', lat: 16.8211, lng: 100.2659 },
  { name: 'Chonburi', lat: 13.3611, lng: 100.9847 },
  { name: 'Rayong', lat: 12.6814, lng: 101.2816 },
  { name: 'Songkhla', lat: 7.1897, lng: 100.5951 },
  { name: 'Phuket', lat: 7.8804, lng: 98.3923 },
] as const

export function countryLabel(code?: string | null) {
  if (!code) return ''
  return COUNTRY_LABELS[code] ?? code
}

export function nearestPlaceLabel(lat: number, lng: number) {
  let nearest: { name: string; distance: number } | null = null

  for (const place of KNOWN_PLACES) {
    const distance = Math.hypot(place.lat - lat, place.lng - lng)
    if (distance <= 0.25 && (!nearest || distance < nearest.distance)) {
      nearest = { name: place.name, distance }
    }
  }

  return nearest?.name ?? ''
}

export interface GeoFilters {
  query: string
  platform: string
  domain: string
  year: string
}

export interface GeoCluster {
  key: string
  lat: number
  lng: number
  items: GeoItem[]
  exactCount: number
  approximateCount: number
  exact: boolean
  city: string | null
  country: string | null
  dominantDomain: string | null
  color: string
  primary: GeoItem
  label: string
}

export function isApproximate(item: GeoItem) {
  return Boolean(item.is_approximate)
}

export function domainLabel(domain?: string | null) {
  if (!domain) return ''
  return DOMAIN_LABELS[domain] ?? domain.replace(/_/g, ' ')
}

export function domainColor(domain?: string | null) {
  if (!domain) return '#67e8f9'
  const index = DOMAIN_ORDER.indexOf(domain)
  return index >= 0 ? DOMAIN_COLORS[index % DOMAIN_COLORS.length] : '#67e8f9'
}

export function platformLabel(platform?: string | null) {
  if (!platform) return ''
  return PLATFORM_LABELS[platform] ?? platform.replace(/_/g, ' ')
}

export function filterGeoItems(items: GeoItem[], filters: GeoFilters) {
  const query = filters.query.trim().toLowerCase()

  return items.filter((item) => {
    if (filters.platform !== 'all' && item.platform !== filters.platform) return false
    if (filters.domain !== 'all' && item.quantum_domain !== filters.domain) return false
    if (filters.year !== 'all' && String(item.published_year ?? '') !== filters.year) return false

    if (!query) return true

    const haystack = [
      item.title,
      item.city,
      item.region,
      item.isp,
      item.channel_title,
      item.channel_country,
      item.quantum_domain,
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()

    return haystack.includes(query)
  })
}

export function aggregateGeoClusters(items: GeoItem[]) {
  const buckets = new Map<string, GeoItem[]>()

  for (const item of items) {
    const key = `${item.lat.toFixed(2)},${item.lng.toFixed(2)}`
    const bucket = buckets.get(key)
    if (bucket) bucket.push(item)
    else buckets.set(key, [item])
  }

  const clusters: GeoCluster[] = []

  for (const [key, bucket] of buckets) {
    const sorted = [...bucket].sort((a, b) => {
      const yearA = Number(a.published_year ?? 0)
      const yearB = Number(b.published_year ?? 0)
      return yearB - yearA
    })
    const exactCount = sorted.filter((item) => !isApproximate(item)).length
    const approximateCount = sorted.length - exactCount
    const domainCounts = new Map<string, number>()

    for (const item of sorted) {
      if (!item.quantum_domain) continue
      domainCounts.set(item.quantum_domain, (domainCounts.get(item.quantum_domain) ?? 0) + 1)
    }

    let dominantDomain: string | null = null
    let dominantCount = 0
    for (const [domain, count] of domainCounts) {
      if (count > dominantCount) {
        dominantDomain = domain
        dominantCount = count
      }
    }

    const primary = sorted.find((item) => !isApproximate(item)) ?? sorted[0]
    const city = sorted.find((item) => item.city)?.city ?? null
    const region = sorted.find((item) => item.region)?.region ?? null
    const country =
      sorted.find((item) => item.channel_country)?.channel_country ?? null
    const exact = exactCount > 0
    const lat = bucket.reduce((sum, item) => sum + item.lat, 0) / bucket.length
    const lng = bucket.reduce((sum, item) => sum + item.lng, 0) / bucket.length
    const nearestPlace = nearestPlaceLabel(lat, lng)
    const label =
      city ??
      region ??
      (!exact && country
        ? `${countryLabel(country)} centroid`
        : nearestPlace || `${lat.toFixed(2)}°, ${lng.toFixed(2)}°`)

    clusters.push({
      key,
      lat,
      lng,
      items: sorted,
      exactCount,
      approximateCount,
      exact,
      city,
      country,
      dominantDomain,
      color: exact ? domainColor(dominantDomain) : '#94a3b8',
      primary,
      label,
    })
  }

  return clusters.sort((a, b) => b.items.length - a.items.length)
}
