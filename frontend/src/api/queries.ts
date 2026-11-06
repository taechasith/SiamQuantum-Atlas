import { useQuery, useMutation, useQueryClient } from '@tanstack/vue-query'
import { computed, type Ref } from 'vue'

export interface SummaryStats {
  total_sources: number
  total_triplets: number
  year_range: [number, number]
  geo_count: number
  platforms: Array<{ platform: string; count: number }>
}

export interface GeoItem {
  source_id: number
  lat: number
  lng: number
  city?: string
  region?: string
  isp?: string
  asn_org?: string
  is_cdn_resolved?: number | null
  platform: string
  url: string
  title: string
  published_year?: number | null
  quantum_domain?: string | null
  channel_title?: string | null
  channel_country?: string | null
  is_approximate?: number | null
  relevance_confidence?: number | null
  fetched_at?: string | null
  view_count?: number | null
  is_quantum_tech?: number | null
  is_thailand_related?: number | null
}

export interface NetworkNode {
  id: string
  label: string
  group?: string
  category?: string
  val?: number
  color?: string
}

export interface NetworkLink {
  source: any
  target: any
  label?: string
  weight?: number
}

export interface NetworkData {
  nodes: NetworkNode[]
  links: NetworkLink[]
}

export interface SourceItem {
  id: number
  title?: string
  url: string
  platform: string
  published_year?: number
  raw_text?: string
  channel_title?: string
  view_count?: number
}

export function useStatsSummaryQuery() {
  return useQuery({
    queryKey: ['stats', 'summary'],
    queryFn: async (): Promise<SummaryStats> => {
      const res = await fetch('/api/stats/summary')
      if (!res.ok) throw new Error('Failed to load stats summary')
      const json = await res.json()
      return json.data
    },
  })
}

export interface YearlyStatsData {
  years: string[]
  counts: Record<string, { total: number; gdelt?: number; youtube?: number; manual_seed?: number; web?: number }>
}

export function useYearlyStatsQuery() {
  return useQuery({
    queryKey: ['stats', 'yearly'],
    queryFn: async (): Promise<YearlyStatsData> => {
      const res = await fetch('/api/stats/yearly')
      if (!res.ok) throw new Error('Failed to load yearly stats')
      const json = await res.json()
      return json.data
    },
  })
}

export interface CorpusCoverageData {
  total: number
  by_year: Record<string, Record<string, number>>
  by_domain: Array<{ domain: string; count: number }>
  years: string[]
}

export function useCorpusCoverageQuery() {
  return useQuery({
    queryKey: ['corpus', 'coverage'],
    queryFn: async (): Promise<CorpusCoverageData> => {
      const res = await fetch('/api/corpus/coverage')
      if (!res.ok) throw new Error('Failed to load corpus coverage')
      const json = await res.json()
      return json.data
    },
  })
}

export function useGeoListQuery(scope: string = 'th') {
  return useQuery({
    queryKey: ['geo', 'list', scope],
    queryFn: async (): Promise<GeoItem[]> => {
      const res = await fetch(`/api/geo/list?scope=${encodeURIComponent(scope)}`)
      if (!res.ok) throw new Error('Failed to load geographic data')
      const json = await res.json()
      return json.data || []
    },
  })
}

export function useNetworkGraphQuery() {
  return useQuery({
    queryKey: ['network', 'graph'],
    queryFn: async (): Promise<NetworkData> => {
      const res = await fetch('/api/graph')
      if (!res.ok) throw new Error('Failed to load network graph')
      const json = await res.json()
      return json.data || { nodes: [], links: [] }
    },
  })
}

export function useSourcesQuery(params: { year?: string; platform?: string; q?: string } = {}) {
  return useQuery({
    queryKey: ['sources', params],
    queryFn: async (): Promise<SourceItem[]> => {
      const buildQuery = (page: number) => {
        const query = new URLSearchParams()
        if (params.year) query.set('year', params.year)
        if (params.platform) query.set('platform', params.platform)
        if (params.q) query.set('search', params.q)
        query.set('page_size', '100')
        query.set('page', String(page))
        return query.toString()
      }

      const res1 = await fetch(`/api/sources?${buildQuery(1)}`)
      if (!res1.ok) throw new Error('Failed to load sources')
      const json1 = await res1.json()
      const total = json1.data?.total || 0
      let items: SourceItem[] = json1.data?.items || (Array.isArray(json1.data) ? json1.data : [])

      if (total > 100) {
        const totalPages = Math.ceil(total / 100)
        const fetchPromises = []
        for (let p = 2; p <= totalPages; p++) {
          fetchPromises.push(
            fetch(`/api/sources?${buildQuery(p)}`)
              .then((r) => r.json())
              .then((j) => j.data?.items || [])
          )
        }
        const rest = await Promise.all(fetchPromises)
        for (const r of rest) {
          items = items.concat(r)
        }
      }

      return items
    },
  })
}

export interface SubmitDataPayload {
  title: string
  url: string
  category: string
  handle?: string
}

export function useSubmitDataMutation() {
  return useMutation({
    mutationFn: async (payload: SubmitDataPayload) => {
      const res = await fetch('/api/submitted-data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: payload.title,
          source_url: payload.url,
          category: payload.category,
          categories: [payload.category],
          metadata: payload.handle ? { submitted_by: payload.handle } : {},
        }),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok || json?.ok === false) {
        throw new Error(json?.error?.message || 'Failed to submit data')
      }
      return json
    },
  })
}

export interface AuthUser {
  id: string
  email: string
  display_name?: string
  role?: string
}

export function useAuthUserQuery() {
  return useQuery({
    queryKey: ['auth', 'me'],
    queryFn: async (): Promise<AuthUser | null> => {
      try {
        const res = await fetch('/api/auth/me')
        if (!res.ok) return null
        const json = await res.json()
        if (!json.ok || !json.data?.user) return null
        return {
          ...json.data.user,
          role: json.data.profile?.role ?? json.data.user.role,
        } as AuthUser
      } catch {
        return null
      }
    },
    staleTime: 1000 * 60 * 5,
  })
}

export function useLocalLoginMutation() {
  return useMutation({
    mutationFn: async (payload: { email: string; password?: string }) => {
      const res = await fetch('/api/auth/local/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: payload.email,
          password: payload.password || 'password123',
        }),
      })
      const json = await res.json()
      if (!res.ok || !json.ok) {
        throw new Error(json?.error?.message || 'Login failed')
      }
      return json.data.user
    },
  })
}

export function useLocalRegisterMutation() {
  return useMutation({
    mutationFn: async (payload: { email: string; password: string }) => {
      const res = await fetch('/api/auth/local/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok || !json.ok) {
        throw new Error(json?.error?.message || 'Registration failed')
      }
      return json.data.user
    },
  })
}

export function useLocalLogoutMutation() {
  return useMutation({
    mutationFn: async () => {
      await fetch('/api/auth/local/logout', { method: 'POST' })
    },
  })
}

export interface MySubmission {
  id: number
  title?: string
  description?: string
  source_url?: string
  category?: string
  page_target?: string
  status?: string
  analysis_status?: string
  metadata?: Record<string, unknown>
  created_at?: string
  updated_at?: string
}

export function useMySubmissionsQuery(enabled?: Ref<boolean>) {
  return useQuery({
    ...(enabled ? { enabled } : {}),
    queryKey: ['submitted-data', 'mine'],
    queryFn: async (): Promise<MySubmission[]> => {
      const res = await fetch('/api/submitted-data/mine?limit=100')
      if (!res.ok) return []
      const json = await res.json().catch(() => null)
      return json?.ok && Array.isArray(json?.data?.items) ? json.data.items : []
    },
    staleTime: 1000 * 30,
  })
}

export interface AdminSubmission extends MySubmission {
  user_id?: string
  submitted_by?: string
}

export function useAdminSubmissionsQuery(status: Ref<string>, category: Ref<string>, enabled: Ref<boolean>) {
  return useQuery({
    queryKey: computed(() => ['admin', 'submissions', status.value, category.value]),
    enabled,
    queryFn: async (): Promise<AdminSubmission[]> => {
      const params = new URLSearchParams()
      if (status.value && status.value !== 'all') params.set('status', status.value)
      if (category.value && category.value !== 'all') params.set('category', category.value)
      params.set('limit', '200')
      const res = await fetch(`/api/admin/submitted-data?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load admin submissions')
      const json = await res.json().catch(() => null)
      return json?.ok && Array.isArray(json?.data?.items) ? json.data.items : []
    },
    staleTime: 1000 * 30,
  })
}

export interface AdminSubmissionPatch {
  status?: string
  analysis_status?: string
  analysis_result?: unknown
}

export function useAdminUpdateSubmissionMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async ({ id, patch }: { id: number; patch: AdminSubmissionPatch }) => {
      const res = await fetch(`/api/admin/submitted-data/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok || json?.ok === false) {
        throw new Error(json?.error?.message || 'Failed to update submission')
      }
      return json
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'submissions'] })
      queryClient.invalidateQueries({ queryKey: ['submitted-data', 'mine'] })
      queryClient.invalidateQueries({ queryKey: ['admin', 'audit-log'] })
    },
  })
}

export interface AdminUser {
  id: string
  email: string
  display_name?: string
  role: string
  created_at?: string
}

export function useAdminUsersQuery(enabled: Ref<boolean>, search: Ref<string>) {
  return useQuery({
    queryKey: computed(() => ['admin', 'users', search.value]),
    enabled,
    queryFn: async (): Promise<AdminUser[]> => {
      const params = new URLSearchParams()
      if (search.value.trim()) params.set('q', search.value.trim())
      params.set('limit', '500')
      const res = await fetch(`/api/admin/users?${params.toString()}`)
      if (!res.ok) throw new Error('Failed to load users')
      const json = await res.json().catch(() => null)
      return json?.ok && Array.isArray(json?.data?.items) ? json.data.items : []
    },
    staleTime: 1000 * 30,
  })
}

export function useAdminUpdateUserRoleMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async ({ id, role }: { id: string; role: 'admin' | 'user' }) => {
      const res = await fetch(`/api/admin/users/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role }),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok || json?.ok === false) {
        throw new Error(json?.error?.message || 'Failed to update role')
      }
      return json
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'users'] })
      queryClient.invalidateQueries({ queryKey: ['admin', 'audit-log'] })
      queryClient.invalidateQueries({ queryKey: ['auth', 'me'] })
    },
  })
}

export interface AdminAuditEvent {
  id: number
  admin_user_id: string
  admin_email?: string
  action: string
  target_type: string
  target_id?: string
  details?: Record<string, unknown>
  created_at: string
}

export function useAdminAuditLogQuery(enabled: Ref<boolean>) {
  return useQuery({
    queryKey: ['admin', 'audit-log'],
    enabled,
    queryFn: async (): Promise<AdminAuditEvent[]> => {
      const res = await fetch('/api/admin/audit-log?limit=100')
      if (!res.ok) throw new Error('Failed to load audit log')
      const json = await res.json().catch(() => null)
      return json?.ok && Array.isArray(json?.data?.items) ? json.data.items : []
    },
    staleTime: 1000 * 30,
  })
}
