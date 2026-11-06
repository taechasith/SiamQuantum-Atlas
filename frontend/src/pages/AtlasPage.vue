<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useGeoListQuery, type GeoItem } from '@/api/queries'
import QuantumGlobe from '@/components/globe/QuantumGlobe.vue'
import GeoMap from '@/components/map/GeoMap.vue'
import NetworkPage from '@/pages/NetworkPage.vue'
import {
  Select as UiSelect,
  SelectContent as UiSelectContent,
  SelectItem as UiSelectItem,
  SelectTrigger as UiSelectTrigger,
  SelectValue as UiSelectValue,
} from '@/components/ui/select'
import { t } from '@/composables/useLocale'
import {
  DOMAIN_ORDER,
  aggregateGeoClusters,
  countryLabel,
  domainColor,
  domainLabel,
  filterGeoItems,
  nearestPlaceLabel,
  platformLabel,
  type GeoCluster,
  type GeoFilters,
} from '@/lib/geo'

type AtlasMode = 'globe' | 'network'

const route = useRoute()
const router = useRouter()
const { data: geoData, isPending, isError, error, refetch } = useGeoListQuery('strict')

const mode = ref<AtlasMode>(route.query.view === 'network' ? 'network' : 'globe')
const isGlobe = computed(() => mode.value === 'globe')
const filters = reactive<GeoFilters>({
  query: '',
  platform: 'all',
  domain: 'all',
  year: 'all',
})
const filtersOpen = ref(false)
const selectedKey = ref<string | null>(null)
const globeRef = ref<InstanceType<typeof QuantumGlobe> | null>(null)
const webglAvailable = ref(canUseWebGL())

const filteredItems = computed(() => filterGeoItems(geoData.value ?? [], filters))
const clusters = computed(() => aggregateGeoClusters(filteredItems.value))
const selectedCluster = computed(() =>
  clusters.value.find((cluster) => cluster.key === selectedKey.value) ?? null
)
const selectedItems = computed(() => selectedCluster.value?.items ?? [])
const selectedSource = ref<GeoItem | null>(null)

const sourceCode = computed(() =>
  selectedSource.value ? `SQA-${selectedSource.value.source_id}` : ''
)
const sourceLocationLabel = computed(() =>
  selectedSource.value ? sourceLocation(selectedSource.value) : ''
)
const sourceDomainLabel = computed(() =>
  selectedSource.value
    ? selectedSource.value.quantum_domain
      ? t(domainLabel(selectedSource.value.quantum_domain))
      : t('Unclassified')
    : ''
)
const sourceFocusTags = computed(() => {
  const item = selectedSource.value
  if (!item) return []
  return [
    item.quantum_domain ? t(domainLabel(item.quantum_domain)) : t('Unclassified'),
    platformLabel(item.platform).toUpperCase(),
    item.city ?? item.region ?? item.channel_country ?? '',
    item.isp ?? item.asn_org ?? '',
  ].filter((value, index, values) => value && values.indexOf(value) === index)
})
const sourceFetchedAgo = computed(() => {
  const fetchedAt = selectedSource.value?.fetched_at
  if (!fetchedAt) return '—'
  const time = new Date(fetchedAt).getTime()
  if (!Number.isFinite(time)) return '—'
  const days = Math.max(0, Math.floor((Date.now() - time) / 86_400_000))
  return `${days}d`
})
const sourceTopology = computed(() => {
  const item = selectedSource.value
  if (!item) return []

  const all = geoData.value ?? []
  const clusterPeers = Math.max(0, (selectedCluster.value?.items.length ?? 1) - 1)
  const platformPeers = Math.max(
    0,
    all.filter((candidate) => candidate.platform === item.platform).length - 1
  )
  const area =
    item.region ||
    item.city ||
    nearestPlaceLabel(item.lat, item.lng) ||
    countryLabel(item.channel_country)
  const region = area || t('Unknown location')
  const regionPeers = Math.max(
    0,
    all.filter((candidate) => {
      if (item.is_approximate) {
        return Boolean(
          candidate.is_approximate &&
            candidate.channel_country === item.channel_country
        )
      }
      if (candidate.is_approximate) return false
      if (item.region) return candidate.region === item.region
      if (item.city) return candidate.city === item.city
      return Boolean(area && nearestPlaceLabel(candidate.lat, candidate.lng) === area)
    }).length - 1
  )
  const rows = [
    {
      title: `${selectedCluster.value?.label ?? sourceLocationLabel.value} ${t('cluster')}`,
      meta: sourceLocationLabel.value,
      peers: clusterPeers,
    },
    {
      title: `${platformLabel(item.platform)} ${t('feed')}`,
      meta: t('Same source type'),
      peers: platformPeers,
    },
    {
      title: `${region} ${t('region')}`,
      meta: region || t('Unknown location'),
      peers: regionPeers,
    },
  ]
  const maxPeers = Math.max(...rows.map((row) => row.peers), 1)

  return rows.map((row) => ({
    ...row,
    width: Math.max(4, Math.round((row.peers / maxPeers) * 100)),
  }))
})
const activeFilterCount = computed(() => {
  let count = 0
  if (filters.query.trim()) count += 1
  if (filters.platform !== 'all') count += 1
  if (filters.domain !== 'all') count += 1
  if (filters.year !== 'all') count += 1
  return count
})

const platforms = computed(() => {
  const values = new Set((geoData.value ?? []).map((item) => item.platform).filter(Boolean))
  return [...values].sort()
})

const domains = computed(() => {
  const values = new Set(
    (geoData.value ?? [])
      .map((item) => item.quantum_domain)
      .filter((value): value is string => Boolean(value))
  )
  return DOMAIN_ORDER.filter((domain) => values.has(domain)).concat(
    [...values].filter((domain) => !DOMAIN_ORDER.includes(domain))
  )
})

const years = computed(() => {
  const values = new Set(
    (geoData.value ?? [])
      .map((item) => item.published_year)
      .filter((value): value is number => typeof value === 'number')
  )
  return [...values].sort((a, b) => b - a)
})

function setMode(nextMode: AtlasMode) {
  mode.value = nextMode
  router.replace({
    path: '/atlas',
    query: { ...route.query, view: nextMode },
  })
}

function resetFilters() {
  filters.query = ''
  filters.platform = 'all'
  filters.domain = 'all'
  filters.year = 'all'
}

function selectCluster(cluster: GeoCluster) {
  selectedKey.value = cluster.key
  selectedSource.value = null
}

function clearSelection() {
  selectedKey.value = null
  selectedSource.value = null
}

function selectSource(item: GeoItem) {
  selectedSource.value = item
}

function clearSelectedSource() {
  selectedSource.value = null
}

function sourceLocation(item: GeoItem) {
  if (item.is_approximate) {
    return item.channel_country
      ? `${countryLabel(item.channel_country)} · ${t('Approximate centroid')}`
      : t('Approximate country centroid')
  }
  const places = [...new Set([item.city, item.region].filter(Boolean))]
  return (
    places.join(', ') ||
    nearestPlaceLabel(item.lat, item.lng) ||
    item.isp ||
    t('Unknown location')
  )
}

function sourceDomain(item: GeoItem) {
  return item.quantum_domain ? t(domainLabel(item.quantum_domain)) : t('Unclassified')
}

function onGeoPointSelect(item: GeoItem) {
  const cluster = clusters.value.find((entry) =>
    entry.items.some((source) => source.source_id === item.source_id)
  )
  if (cluster) selectCluster(cluster)
}

function onGeoSourceSelect(item: GeoItem) {
  const cluster = clusters.value.find((entry) =>
    entry.items.some((source) => source.source_id === item.source_id)
  )
  if (!cluster) return
  selectCluster(cluster)
  selectedSource.value = item
}

function canUseWebGL() {
  if (typeof document === 'undefined') return false
  try {
    const canvas = document.createElement('canvas')
    return Boolean(
      window.WebGLRenderingContext &&
        (canvas.getContext('webgl') || canvas.getContext('experimental-webgl'))
    )
  } catch {
    return false
  }
}

watch(
  () => route.query.view,
  (view) => {
    mode.value = view === 'network' ? 'network' : 'globe'
  }
)

watch(clusters, (nextClusters) => {
  if (selectedKey.value && !nextClusters.some((cluster) => cluster.key === selectedKey.value)) {
    selectedKey.value = null
    selectedSource.value = null
  }
  if (
    selectedSource.value &&
    !nextClusters.some((cluster) =>
      cluster.items.some((item) => item.source_id === selectedSource.value?.source_id)
    )
  ) {
    selectedSource.value = null
  }
})

watch(mode, () => {
  selectedKey.value = null
  selectedSource.value = null
})
</script>

<template>
  <div class="relative h-full w-full overflow-hidden bg-black text-white">
    <template v-if="isGlobe">
      <QuantumGlobe
        v-if="webglAvailable"
        ref="globeRef"
        :clusters="clusters"
        :selected-key="selectedKey"
        class="absolute inset-0"
        @select-cluster="selectCluster"
      />
      <GeoMap
        v-else
        :points="filteredItems"
        :selected-id="selectedCluster?.primary.source_id ?? null"
        height="100%"
        class="absolute inset-0"
        @select-point="onGeoPointSelect"
        @select-source="onGeoSourceSelect"
      />

      <div class="pointer-events-none absolute inset-0 z-20">
        <header class="flex flex-wrap items-start justify-between gap-3 p-5 md:p-7">
          <div class="pointer-events-auto flex items-center gap-3">
            <h1 class="text-2xl font-black tracking-tight text-white sm:text-[36px] leading-tight m-0 uppercase">SIAM QUANTUM ATLAS</h1>
            <button
              type="button"
              class="rounded-full border border-white/15 bg-black/35 px-2.5 py-1 font-mono text-[10px] text-white/75 transition hover:border-white/35 hover:text-white lg:hidden"
              @click="filtersOpen = !filtersOpen"
            >
              {{ t('Filters') }}{{ activeFilterCount ? ` (${activeFilterCount})` : '' }}
            </button>
          </div>

          <div class="pointer-events-auto flex items-center gap-1 atlas-ci-bar rounded-2xl p-1">
            <button
              type="button"
              class="rounded-xl px-3 py-2 text-xs font-bold transition"
              :class="isGlobe ? 'bg-white text-slate-950' : 'text-white/60 hover:text-white'"
              @click="setMode('globe')"
            >
              {{ t('Globe') }}
            </button>
            <button
              type="button"
              class="rounded-xl px-3 py-2 text-xs font-bold transition"
              :class="!isGlobe ? 'bg-white text-slate-950' : 'text-white/60 hover:text-white'"
              @click="setMode('network')"
            >
              {{ t('Network') }}
            </button>
          </div>
        </header>

        <aside
          class="pointer-events-auto absolute left-5 top-32 w-[calc(100%-2.5rem)] max-w-[19rem] atlas-ci-panel rounded-[26px] p-4 text-white md:left-7 md:top-24"
          :class="filtersOpen ? 'block' : 'hidden lg:block'"
        >
          <div class="flex items-center justify-between gap-3">
            <h2 class="font-mono text-[10px] font-bold uppercase tracking-[0.2em] text-sky-200/70">{{ t('Filters') }}</h2>
            <div class="flex items-center gap-3">
              <button
                v-if="activeFilterCount"
                type="button"
                class="text-[10px] font-mono text-white/45 transition hover:text-white"
                @click="resetFilters"
              >
                {{ t('Reset') }}
              </button>
              <button
                type="button"
                class="flex h-6 w-6 items-center justify-center rounded-full bg-white/10 text-[10px] text-white/65 transition hover:bg-white/20 hover:text-white lg:hidden"
                :aria-label="t('Close')"
                @click="filtersOpen = false"
              >
                ✕
              </button>
            </div>
          </div>

          <label class="mt-3 block">
            <span class="mb-1.5 block text-[10px] font-mono uppercase tracking-wider text-white/40">{{ t('Search') }}</span>
            <input
              v-model="filters.query"
              type="search"
              :placeholder="t('Search sources, cities, channels...')"
              class="h-10 w-full rounded-xl border border-white/10 bg-black/35 px-3 text-xs text-white outline-none transition placeholder:text-white/30 focus:border-sky-300/40"
            />
          </label>

          <div class="mt-3 grid grid-cols-1 gap-3">
            <label class="block">
              <span class="mb-1.5 block text-[10px] font-mono uppercase tracking-wider text-white/40">{{ t('Platform') }}</span>
              <UiSelect v-model="filters.platform">
                <UiSelectTrigger class="h-10">
                  <UiSelectValue />
                </UiSelectTrigger>
                <UiSelectContent>
                  <UiSelectItem value="all">{{ t('All platforms') }}</UiSelectItem>
                  <UiSelectItem v-for="platform in platforms" :key="platform" :value="platform">
                    {{ t(platformLabel(platform)) }}
                  </UiSelectItem>
                </UiSelectContent>
              </UiSelect>
            </label>

            <label class="block">
              <span class="mb-1.5 block text-[10px] font-mono uppercase tracking-wider text-white/40">{{ t('Domain') }}</span>
              <UiSelect v-model="filters.domain">
                <UiSelectTrigger class="h-10">
                  <UiSelectValue />
                </UiSelectTrigger>
                <UiSelectContent>
                  <UiSelectItem value="all">{{ t('All domains') }}</UiSelectItem>
                  <UiSelectItem v-for="domain in domains" :key="domain" :value="domain">
                    {{ t(domainLabel(domain)) }}
                  </UiSelectItem>
                </UiSelectContent>
              </UiSelect>
            </label>

            <label class="block">
              <span class="mb-1.5 block text-[10px] font-mono uppercase tracking-wider text-white/40">{{ t('Year') }}</span>
              <UiSelect v-model="filters.year">
                <UiSelectTrigger class="h-10">
                  <UiSelectValue />
                </UiSelectTrigger>
                <UiSelectContent>
                  <UiSelectItem value="all">{{ t('All years') }}</UiSelectItem>
                  <UiSelectItem v-for="year in years" :key="year" :value="String(year)">
                    {{ year }}
                  </UiSelectItem>
                </UiSelectContent>
              </UiSelect>
            </label>
          </div>

          <div class="mt-4 flex items-center justify-between border-t border-white/10 pt-3 font-mono text-[10px] text-white/45">
            <span>{{ clusters.length }} {{ t('clusters') }}</span>
            <span>{{ filteredItems.length }} {{ t('sources') }}</span>
          </div>
        </aside>

        <section
          class="pointer-events-auto absolute bottom-5 left-5 right-5 z-30 max-h-[42%] flex-col overflow-hidden atlas-ci-panel rounded-[26px] text-white md:bottom-auto md:right-7 md:left-auto md:top-24 md:max-h-[calc(100%-9rem)] md:w-[22rem]"
          :class="selectedCluster ? 'flex' : 'hidden md:flex'"
        >
          <template v-if="selectedCluster && selectedSource">
            <div class="flex items-center justify-between gap-2 px-3 py-2.5">
              <button
                type="button"
                class="rounded-lg px-2 py-1.5 font-mono text-[11px] text-white/60 transition hover:bg-white/10 hover:text-white"
                @click="clearSelectedSource"
              >
                ← {{ t('Back to sources') }}
              </button>
              <span class="font-mono text-[10px] font-bold uppercase tracking-[0.18em] text-sky-200/60">{{ t('Details') }}</span>
              <button
                type="button"
                class="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-full bg-white/10 text-sm text-white/70 transition hover:bg-white/20 hover:text-white"
                :aria-label="t('Close')"
                @click="clearSelection"
              >
                ✕
              </button>
            </div>

            <div class="min-h-0 flex-1 overflow-y-auto p-4">
              <div class="flex items-center gap-2 font-mono text-[9px] font-bold uppercase tracking-[0.16em] text-emerald-300">
                <span class="h-1.5 w-1.5 rounded-full bg-emerald-300 shadow-[0_0_8px_rgba(110,231,183,0.85)]" />
                {{ t('Online • Synced') }}
              </div>
              <h2 class="mt-2 text-base font-black leading-snug text-white">{{ selectedSource.title }}</h2>
              <p class="mt-1.5 font-mono text-[10px] text-white/45">{{ sourceCode }} • {{ sourceLocationLabel }}</p>

              <div
                v-if="selectedSource.is_approximate"
                class="mt-3 rounded-lg bg-amber-300/10 px-2.5 py-1.5 text-[10px] leading-relaxed text-amber-100/80"
              >
                {{ t('Country-level location only') }}
              </div>

              <section class="mt-4">
                <p class="font-mono text-[9px] font-bold uppercase tracking-[0.18em] text-sky-200/60">{{ t('Domain Focus') }}</p>
                <div class="mt-2 flex flex-wrap gap-1.5">
                  <span
                    v-for="tag in sourceFocusTags"
                    :key="tag"
                    class="rounded-md bg-white/[0.06] px-2 py-1 font-mono text-[9px] font-bold text-white/75"
                  >
                    {{ tag }}
                  </span>
                </div>
              </section>

              <section class="mt-4">
                <p class="font-mono text-[9px] font-bold uppercase tracking-[0.18em] text-sky-200/60">{{ t('Live Telemetry') }}</p>
                <div class="mt-2 grid grid-cols-2 gap-2">
                  <div class="rounded-lg bg-white/[0.035] px-2.5 py-2">
                    <p class="font-mono text-[8px] uppercase tracking-wider text-white/35">{{ t('Source ID') }}</p>
                    <p class="mt-0.5 font-mono text-xs font-bold text-white/85">{{ sourceCode }}</p>
                  </div>
                  <div class="rounded-lg bg-white/[0.035] px-2.5 py-2">
                    <p class="font-mono text-[8px] uppercase tracking-wider text-white/35">{{ t('Platform') }}</p>
                    <p class="mt-0.5 font-mono text-xs font-bold uppercase text-white/85">{{ selectedSource.platform }}</p>
                  </div>
                  <div class="rounded-lg bg-white/[0.035] px-2.5 py-2">
                    <p class="font-mono text-[8px] uppercase tracking-wider text-white/35">{{ t('Published') }}</p>
                    <p class="mt-0.5 font-mono text-xs font-bold text-white/85">{{ selectedSource.published_year ?? '—' }}</p>
                  </div>
                  <div class="rounded-lg bg-white/[0.035] px-2.5 py-2">
                    <p class="font-mono text-[8px] uppercase tracking-wider text-white/35">{{ t('Last Fetch') }}</p>
                    <p class="mt-0.5 font-mono text-xs font-bold text-emerald-300">{{ sourceFetchedAgo }}</p>
                  </div>
                  <div class="col-span-2 rounded-lg bg-white/[0.035] px-2.5 py-2">
                    <p class="font-mono text-[8px] uppercase tracking-wider text-white/35">{{ t('Quantum Domain') }}</p>
                    <p class="mt-0.5 text-xs font-bold text-sky-200">{{ sourceDomainLabel }}</p>
                  </div>
                </div>
              </section>

              <section class="mt-4">
                <p class="font-mono text-[9px] font-bold uppercase tracking-[0.18em] text-sky-200/60">{{ t('Network Topology') }}</p>
                <div class="mt-2 space-y-2">
                  <div v-for="row in sourceTopology" :key="row.title" class="rounded-lg bg-white/[0.03] px-2.5 py-2">
                    <p class="truncate text-[11px] font-bold text-white/80">{{ row.title }}</p>
                    <div class="mt-1 flex items-center justify-between gap-2 font-mono text-[8px] text-white/35">
                      <span class="truncate">{{ row.meta }}</span>
                      <span class="flex-shrink-0">{{ row.peers }} {{ t('peers') }}</span>
                    </div>
                    <div class="mt-1.5 h-1 overflow-hidden rounded-full bg-white/10">
                      <div class="h-full rounded-full bg-sky-300/70" :style="{ width: `${row.width}%` }" />
                    </div>
                  </div>
                </div>
              </section>

              <div class="mt-4 grid grid-cols-2 gap-2 pb-1">
                <a
                  :href="selectedSource.url"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="inline-flex min-h-[38px] items-center justify-center rounded-lg bg-white px-2.5 py-2 text-center text-[11px] font-bold text-slate-950 transition hover:bg-sky-100"
                >
                  {{ t('Open Source') }}
                </a>
                <router-link
                  :to="{ path: '/database', query: { q: selectedSource.title } }"
                  class="inline-flex min-h-[38px] items-center justify-center rounded-lg bg-white/[0.08] px-2.5 py-2 text-center text-[11px] font-bold text-white/80 transition hover:bg-white/15 hover:text-white"
                >
                  {{ t('View Library') }}
                </router-link>
              </div>
            </div>
          </template>

          <template v-else-if="selectedCluster">
            <div class="flex items-start justify-between gap-3 p-4">
              <div class="min-w-0">
                <p class="font-mono text-[10px] uppercase tracking-[0.18em] text-sky-200/65">
                  {{ selectedCluster.exact ? t('Selected location') : t('Approximate location') }}
                </p>
                <h2 class="mt-1 truncate text-lg font-black tracking-tight">{{ selectedCluster.label }}</h2>
                <p class="mt-1 font-mono text-[10px] text-white/45">
                  {{ selectedCluster.items.length }} {{ selectedCluster.items.length === 1 ? t('source') : t('sources') }}
                  <span v-if="selectedCluster.exact && selectedCluster.approximateCount">
                    · {{ selectedCluster.approximateCount }} {{ t('approximate') }}
                  </span>
                </p>
                <p
                  v-if="selectedCluster.approximateCount === selectedCluster.items.length"
                  class="mt-2 rounded-lg bg-amber-300/10 px-2 py-1.5 text-[10px] leading-relaxed text-amber-100/75"
                >
                  {{ t('All sources in this cluster share the same country centroid.') }}
                </p>
              </div>
              <button
                type="button"
                class="flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-full bg-white/10 text-xs text-white/70 transition hover:bg-white/20 hover:text-white"
                :aria-label="t('Close')"
                @click="clearSelection"
              >
                ✕
              </button>
            </div>

            <div class="min-h-0 flex-1 overflow-y-auto p-3">
              <div class="mb-2 flex items-center gap-2 px-1">
                <span class="h-2.5 w-2.5 rounded-full" :style="{ background: domainColor(selectedCluster.dominantDomain) }" />
                <span class="text-xs font-bold">{{ t(domainLabel(selectedCluster.dominantDomain)) }}</span>
                <span v-if="selectedCluster.approximateCount" class="ml-auto rounded-full bg-white/10 px-2 py-0.5 font-mono text-[9px] text-white/65">
                  {{ t('approximate') }}
                </span>
              </div>

              <div class="space-y-2">
                <button
                  v-for="item in selectedItems"
                  :key="item.source_id"
                  type="button"
                  class="group flex w-full items-start gap-2 rounded-xl bg-white/[0.035] p-3 text-left transition hover:bg-white/[0.07]"
                  @click="selectSource(item)"
                >
                  <span class="min-w-0 flex-1">
                    <span class="line-clamp-2 block text-xs font-semibold leading-snug text-white/90">{{ item.title }}</span>
                    <span class="mt-2 flex flex-wrap items-center gap-1.5 font-mono text-[9px] text-white/45">
                      <span class="rounded bg-white/[0.06] px-1.5 py-0.5 text-white/60">{{ t(platformLabel(item.platform)) }}</span>
                      <span>{{ item.published_year ?? '—' }}</span>
                      <span class="truncate">{{ sourceLocation(item) }}</span>
                      <span v-if="item.is_approximate" class="rounded bg-amber-300/10 px-1.5 py-0.5 text-amber-200/70">{{ t('approximate') }}</span>
                    </span>
                    <span class="mt-1.5 block truncate font-mono text-[9px] text-sky-200/55">{{ sourceDomain(item) }}</span>
                  </span>
                  <span class="mt-0.5 flex-shrink-0 font-mono text-xs text-sky-200/45 transition group-hover:text-sky-200">→</span>
                </button>
              </div>
            </div>
          </template>

          <template v-else>
            <div class="min-h-0 flex-1 overflow-y-auto p-3">
              <div class="mb-2 flex items-center justify-between px-1">
                <div>
                  <p class="font-mono text-[10px] uppercase tracking-[0.18em] text-sky-200/65">{{ t('Source clusters') }}</p>
                  <p class="mt-1 text-xs text-white/45">{{ t('Click a cluster to inspect its sources.') }}</p>
                </div>
                <span class="rounded-full bg-white/10 px-2 py-0.5 font-mono text-[9px] text-white/55">{{ clusters.length }}</span>
              </div>

              <div v-if="clusters.length" class="space-y-2">
                <button
                  v-for="cluster in clusters"
                  :key="cluster.key"
                  type="button"
                  class="flex w-full items-center gap-3 rounded-xl bg-white/[0.035] p-3 text-left transition hover:bg-white/[0.07]"
                  @click="selectCluster(cluster)"
                >
                  <span
                    class="h-2.5 w-2.5 flex-shrink-0 rounded-full"
                    :style="{ background: cluster.exact ? domainColor(cluster.dominantDomain) : '#94a3b8' }"
                  />
                  <span class="min-w-0 flex-1">
                    <span class="block truncate text-xs font-bold text-white/90">{{ cluster.label }}</span>
                    <span class="mt-0.5 block font-mono text-[9px] text-white/45">
                      {{ cluster.items.length }} {{ cluster.items.length === 1 ? t('source') : t('sources') }}
                      <span v-if="cluster.approximateCount"> · {{ cluster.approximateCount }} {{ t('approximate') }}</span>
                    </span>
                  </span>
                  <span class="font-mono text-xs text-sky-200/55">→</span>
                </button>
              </div>

              <div v-else class="flex flex-col items-center justify-center px-4 py-10 text-center">
                <span class="flex h-10 w-10 items-center justify-center rounded-full bg-sky-300/10 text-sky-200">
                  <svg viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="1.8">
                    <circle cx="12" cy="10" r="3" />
                    <path d="M19 10c0 5-7 11-7 11S5 15 5 10a7 7 0 1 1 14 0Z" />
                  </svg>
                </span>
                <p class="mt-3 text-sm font-bold">{{ t('No source clusters match these filters') }}</p>
              </div>
            </div>
          </template>
        </section>

        <div class="pointer-events-none absolute bottom-5 left-5 z-30 hidden items-center gap-3 atlas-ci-bar rounded-xl px-3 py-2 font-mono text-[9px] text-white/55 md:flex">
          <span class="flex items-center gap-1.5">
            <span class="h-2 w-2 rounded-full bg-sky-400 shadow-[0_0_8px_rgba(56,189,248,0.8)]" />
            {{ t('Exact location') }}
          </span>
          <span class="h-3 w-px bg-white/15" />
          <span class="flex items-center gap-1.5">
            <span class="h-2 w-2 rounded-full bg-amber-300 shadow-[0_0_8px_rgba(251,191,36,0.8)]" />
            {{ t('Country-level approximation') }}
          </span>
          <span class="text-white/25">·</span>
          <span>{{ t('Expands at high zoom') }}</span>
        </div>

        <div class="pointer-events-auto absolute bottom-5 right-5 flex flex-col items-end gap-2 md:right-7">
          <p class="hidden font-mono text-[9px] text-white/35 sm:block">{{ t('Drag to rotate · Scroll to zoom') }}</p>
          <div class="flex items-center gap-1 atlas-ci-bar rounded-xl p-1">
            <button
              type="button"
              class="rounded-lg px-2.5 py-2 text-[10px] font-bold text-white/65 transition hover:bg-white/10 hover:text-white"
              @click="globeRef?.focusThailand()"
            >
              {{ t('Focus Thailand') }}
            </button>
            <button
              type="button"
              class="rounded-lg px-2.5 py-2 text-[10px] font-bold text-white/65 transition hover:bg-white/10 hover:text-white"
              @click="globeRef?.resetView()"
            >
              {{ t('Reset view') }}
            </button>
          </div>
        </div>

        <div
          v-if="isPending"
          class="pointer-events-auto absolute inset-0 z-40 flex items-center justify-center bg-black/55 backdrop-blur-sm"
        >
          <div class="flex items-center gap-3 rounded-2xl border border-white/10 bg-[#040914]/90 px-4 py-3">
            <span class="h-4 w-4 animate-spin rounded-full border-2 border-sky-300 border-t-transparent" />
            <span class="font-mono text-xs text-white/70">{{ t('Loading geospatial sources...') }}</span>
          </div>
        </div>

        <div
          v-if="isError"
          class="pointer-events-auto absolute left-1/2 top-1/2 z-40 w-[min(90vw,26rem)] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-rose-300/20 bg-[#14060b]/95 p-5 text-center shadow-2xl"
        >
          <p class="text-sm font-bold">{{ t('Could not load geospatial sources') }}</p>
          <p class="mt-1 text-xs text-white/50">{{ error?.message }}</p>
          <button
            type="button"
            class="mt-4 rounded-xl bg-white px-4 py-2 text-xs font-bold text-slate-950"
            @click="() => refetch()"
          >
            {{ t('Retry') }}
          </button>
        </div>
      </div>
    </template>

    <template v-else>
      <div class="absolute inset-0">
        <NetworkPage />
      </div>
      <div class="pointer-events-none absolute inset-x-0 top-0 z-40 flex items-start justify-between gap-3 p-4 md:p-5">
        <h1 class="pointer-events-auto hidden whitespace-nowrap text-2xl font-black tracking-tight text-white sm:block md:text-3xl">
          SIAM QUANTUM ATLAS
        </h1>
        <div class="pointer-events-auto flex flex-shrink-0 items-center gap-1 atlas-ci-bar rounded-2xl p-1">
          <button
            type="button"
            class="whitespace-nowrap rounded-xl px-3 py-2 text-xs font-bold transition text-white/60 hover:text-white"
            @click="setMode('globe')"
          >
            {{ t('Globe') }}
          </button>
          <button
            type="button"
            class="whitespace-nowrap rounded-xl bg-white px-3 py-2 text-xs font-bold text-slate-950"
          >
            {{ t('Network') }}
          </button>
        </div>
      </div>
    </template>
  </div>
</template>


<style scoped>
.atlas-ci-panel {
  background: #0b2a68;
}

.atlas-ci-bar {
  background: rgba(5, 16, 40, 0.72);
  backdrop-filter: blur(18px);
}
</style>
