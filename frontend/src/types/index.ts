export interface SourceRecord {
  id: number
  platform: 'gdelt' | 'youtube' | 'rss' | 'seed'
  url: string
  title: string
  raw_text?: string
  published_year: number
  fetched_at: string
  view_count?: number
  like_count?: number
  comment_count?: number
}

export interface GeoPoint {
  source_id: number
  lat: number
  lng: number
  city?: string
  region?: string
  isp?: string
  asn_org?: string
  is_cdn_resolved?: boolean
}

export interface GraphNode {
  id: string
  label: string
  group?: string
  val?: number
  color?: string
}

export interface GraphLink {
  source: any
  target: any
  label?: string
  weight?: number
}

export interface NetworkGraphData {
  nodes: GraphNode[]
  links: GraphLink[]
}
