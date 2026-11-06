<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useSourcesQuery, type SourceItem } from '@/api/queries'
import Skeleton from '@/components/ui/Skeleton.vue'
import EmptyState from '@/components/ui/EmptyState.vue'
import AppTooltip from '@/components/ui/AppTooltip.vue'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { t } from '@/composables/useLocale'

const route = useRoute()
const { data: sources, isPending, isError, error: sourcesError, refetch } = useSourcesQuery()

const searchQuery = ref(String(route.query.q ?? ''))
const selectedPlatform = ref('all')
const selectedYear = ref('all')
const currentPage = ref(1)
const pageSize = 15

type ViewMode = 'table' | 'card'
const viewMode = ref<ViewMode>(
  typeof localStorage !== 'undefined' && localStorage.getItem('sq_db_view') === 'card' ? 'card' : 'table'
)
watch(viewMode, (mode) => localStorage.setItem('sq_db_view', mode))

const filteredSources = computed(() => {
  if (!sources.value) return []
  return sources.value.filter((item: SourceItem) => {
    const matchSearch =
      !searchQuery.value ||
      item.title?.toLowerCase().includes(searchQuery.value.toLowerCase()) ||
      item.url?.toLowerCase().includes(searchQuery.value.toLowerCase()) ||
      item.raw_text?.toLowerCase().includes(searchQuery.value.toLowerCase())

    const matchPlatform = selectedPlatform.value === 'all' || item.platform === selectedPlatform.value
    const matchYear = selectedYear.value === 'all' || String(item.published_year) === selectedYear.value

    return matchSearch && matchPlatform && matchYear
  })
})

const totalPages = computed(() => Math.ceil(filteredSources.value.length / pageSize) || 1)

const paginatedSources = computed(() => {
  const start = (currentPage.value - 1) * pageSize
  return filteredSources.value.slice(start, start + pageSize)
})

const shownStart = computed(() =>
  filteredSources.value.length === 0 ? 0 : (currentPage.value - 1) * pageSize + 1
)
const shownEnd = computed(() => Math.min(currentPage.value * pageSize, filteredSources.value.length))

watch([searchQuery, selectedPlatform, selectedYear], () => {
  currentPage.value = 1
})

watch(
  () => route.query.q,
  (value) => {
    const next = String(value ?? '')
    if (next !== searchQuery.value) {
      searchQuery.value = next
      currentPage.value = 1
    }
  }
)

watch(totalPages, (pages) => {
  if (currentPage.value > pages) currentPage.value = pages
})

const platformNames: Record<string, string> = {
  youtube: 'YouTube',
  gdelt: 'GDELT',
  manual_seed: 'Manual Seed',
  web: 'Web',
}

const failedThumbs = ref<Set<number>>(new Set())

function platformName(platform: string): string {
  return t(platformNames[platform] ?? platform)
}

function youtubeThumb(url: string): string | null {
  const match = url.match(
    /(?:youtube\.com\/(?:watch\?(?:.*&)?v=|embed\/|shorts\/|live\/)|youtu\.be\/)([A-Za-z0-9_-]{11})/
  )
  return match ? `https://img.youtube.com/vi/${match[1]}/hqdefault.jpg` : null
}

function thumbnailFor(item: SourceItem): string | null {
  if (failedThumbs.value.has(item.id)) return null
  if (item.platform !== 'youtube') return null
  return youtubeThumb(item.url)
}

const cardSources = computed(() =>
  paginatedSources.value.map((item) => ({ ...item, thumb: thumbnailFor(item) }))
)

function markThumbFailed(id: number): void {
  failedThumbs.value.add(id)
}

function hostOf(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, '')
  } catch {
    return url
  }
}

function formatCount(value?: number): string {
  if (!value) return ''
  return new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 }).format(value)
}

function coverTint(platform: string): string {
  switch (platform) {
    case 'youtube':
      return 'bg-gradient-to-br from-rose-500/30 via-slate-950 to-black'
    case 'gdelt':
      return 'bg-gradient-to-br from-sky-500/30 via-slate-950 to-black'
    case 'manual_seed':
      return 'bg-gradient-to-br from-amber-500/30 via-slate-950 to-black'
    default:
      return 'bg-gradient-to-br from-cyan-500/30 via-slate-950 to-black'
  }
}
</script>

<template>
  <div class="space-y-6 select-none w-full max-w-[1920px] mx-auto">
    <!-- Header Controls: sticky while scrolling the corpus -->
    <div class="sticky -top-5 z-30 -mx-[calc(1.25rem+5px)] -mt-5 border-b border-white/[0.06] bg-[#070709]/92 px-[calc(1.25rem+5px)] pb-4 pt-5 backdrop-blur-xl md:-top-7 md:-mx-[calc(1.75rem+5px)] md:-mt-7 md:px-[calc(1.75rem+5px)] md:pt-7">
      <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
      <div class="flex flex-wrap items-center gap-2.5">
        <h1 class="text-2xl sm:text-[36px] font-black text-white tracking-tight leading-tight m-0 uppercase">{{ t('Quantum Corpus Database') }}</h1>

        <!-- Info (i) Tooltip Button -->
        <AppTooltip
          :content="t('Raw Archive & NLP Records — Raw archive and NLP ingested records from academic journals, news, and official press releases')"
          side="bottom"
        >
          <button
            type="button"
            class="inline-flex h-8 w-8 items-center justify-center rounded-full text-white/50 hover:text-cyan-300 hover:bg-white/10 transition cursor-pointer"
            :aria-label="t('More information about the corpus')"
          >
            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="10" />
              <path d="M12 16v-4" />
              <path d="M12 8h.01" />
            </svg>
          </button>
        </AppTooltip>

        <span class="px-2.5 py-0.5 rounded-full text-xs font-mono font-bold bg-black/55 text-white">
          {{ filteredSources.length }} {{ t('Documents') }}
        </span>
      </div>

      <div class="flex flex-wrap items-center gap-2.5">
        <div class="flex items-center rounded-full bg-white/[0.06] border border-white/[0.08] p-1">
          <button
            type="button"
            @click="viewMode = 'table'"
            :aria-pressed="viewMode === 'table'"
            class="px-3.5 py-1.5 rounded-full font-mono text-[11px] font-bold uppercase tracking-wider transition cursor-pointer"
            :class="viewMode === 'table' ? 'bg-white text-slate-950' : 'text-slate-300 hover:text-white'"
          >
            {{ t('Table') }}
          </button>
          <button
            type="button"
            @click="viewMode = 'card'"
            :aria-pressed="viewMode === 'card'"
            class="px-3.5 py-1.5 rounded-full font-mono text-[11px] font-bold uppercase tracking-wider transition cursor-pointer"
            :class="viewMode === 'card' ? 'bg-white text-slate-950' : 'text-slate-300 hover:text-white'"
          >
            {{ t('Card') }}
          </button>
        </div>
        <a
          href="/api/export/xlsx"
          target="_blank"
          class="px-4 py-2 rounded-full bg-blue-600/30 hover:bg-blue-600/50 text-cyan-300 font-mono font-bold text-xs transition flex items-center gap-2 cursor-pointer"
        >
          <span>{{ t('Download XLSX') }}</span>
          <span>↓</span>
        </a>
      </div>
      </div>
    </div>

    <!-- Table Card Container (Royal Sapphire CI: Zero Shadow, Zero Stroke) -->
    <div
      class="w-full rounded-[30px] xl:rounded-[34px] p-5 sm:p-6 text-white overflow-hidden space-y-4"
      style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
    >
      <!-- Filters Bar inside Card -->
      <div class="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3 p-3 rounded-2xl bg-black/30 w-full">
        <div class="relative flex-1">
          <input
            v-model="searchQuery"
            type="text"
            :placeholder="t('Search by keywords, title, author, URL...')"
            class="w-full px-4 py-2.5 rounded-xl bg-white/[0.08] hover:bg-white/[0.12] focus:bg-white/[0.16] text-xs text-white placeholder-white/50 focus:outline-none transition"
          />
        </div>

        <div class="flex flex-col sm:flex-row sm:items-center gap-2">
          <Select v-model="selectedPlatform">
            <SelectTrigger class="w-full sm:w-[178px]">
              <SelectValue :placeholder="t('All Platforms')" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">{{ t('All Platforms') }}</SelectItem>
              <SelectItem value="youtube">YouTube</SelectItem>
              <SelectItem value="gdelt">GDELT</SelectItem>
              <SelectItem value="manual_seed">{{ t('Manual Seed') }}</SelectItem>
              <SelectItem value="web">{{ t('Web') }}</SelectItem>
            </SelectContent>
          </Select>

          <Select v-model="selectedYear">
            <SelectTrigger class="w-full sm:w-[130px]">
              <SelectValue :placeholder="t('All Years')" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">{{ t('All Years') }}</SelectItem>
              <SelectItem v-for="year in ['2026','2025','2024','2023','2022','2021','2020']" :key="year" :value="year">
                {{ year }}
              </SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <!-- Table Viewport -->
      <div class="rounded-2xl bg-black/25 overflow-hidden w-full">
        <div v-if="viewMode === 'table'" class="overflow-x-auto w-full">
          <table class="w-full text-left text-xs">
            <thead class="bg-black/55 text-white/90 uppercase font-mono tracking-wider text-[11px]">
              <tr>
                <th class="p-3.5 pl-6 w-24">{{ t('ID') }}</th>
                <th class="p-3.5">{{ t('Title / Subject') }}</th>
                <th class="p-3.5 w-36">{{ t('Platform') }}</th>
                <th class="p-3.5 w-24">{{ t('Year') }}</th>
                <th class="p-3.5 text-right pr-6 w-32">{{ t('Provenance') }}</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-white/[0.06]">
              <tr v-if="isPending">
                <td colspan="5" class="p-8 text-center text-white/60">
                  <div class="space-y-3 max-w-md mx-auto">
                    <Skeleton v-for="i in 5" :key="i" variant="text" />
                  </div>
                </td>
              </tr>
              <tr v-else-if="isError">
                <td colspan="5" class="p-8 text-center text-xs text-rose-200">
                  <p class="font-bold m-0">{{ t('Unable to load source data') }}</p>
                  <p class="mt-1 text-white/50">{{ sourcesError?.message }}</p>
                  <button type="button" class="mt-3 rounded-lg bg-white px-3 py-1.5 text-[10px] font-bold text-slate-950" @click="refetch()">{{ t('Retry') }}</button>
                </td>
              </tr>
              <tr v-else-if="!paginatedSources.length">
                <td colspan="5" class="p-8">
                  <EmptyState
                    :title="t('No Matching Records')"
                    :message="t('No quantum sources found matching the current search parameters.')"
                  />
                </td>
              </tr>
              <tr
                v-for="item in paginatedSources"
                :key="item.id"
                class="hover:bg-white/[0.05] transition group"
              >
                <td class="p-3.5 pl-6 font-mono text-cyan-300/70 text-[11px] whitespace-nowrap">#{{ item.id }}</td>
                <td class="p-3.5">
                  <div class="font-medium text-white group-hover:text-cyan-300 transition text-[13px] leading-snug">
                    {{ item.title || item.url }}
                  </div>
                  <div v-if="item.raw_text" class="text-[11.5px] text-sky-200/60 line-clamp-1 mt-0.5">
                    {{ item.raw_text }}
                  </div>
                </td>
                <td class="p-3.5 whitespace-nowrap">
                  <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-[11px] font-mono font-bold bg-black/55 text-white">
                    {{ platformName(item.platform) }}
                  </span>
                </td>
                <td class="p-3.5 font-mono text-white/80 whitespace-nowrap">{{ item.published_year }}</td>
                <td class="p-3.5 text-right pr-6 whitespace-nowrap">
                  <a
                    :href="item.url"
                    target="_blank"
                    class="text-cyan-300 hover:text-white hover:underline font-mono text-[11px] font-semibold transition"
                  >
                    {{ t('Source') }} ↗
                  </a>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Card Viewport -->
        <div v-else class="p-4 sm:p-5">
          <div v-if="isPending" class="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-4">
            <div
              v-for="i in 8"
              :key="i"
              class="rounded-2xl border border-white/[0.04] bg-black/30 overflow-hidden"
            >
              <div class="aspect-video animate-pulse bg-white/[0.04]" />
              <div class="p-4 space-y-2">
                <div class="h-3 w-3/4 animate-pulse rounded bg-white/[0.06]" />
                <div class="h-3 w-1/2 animate-pulse rounded bg-white/[0.04]" />
              </div>
            </div>
          </div>

          <div v-else-if="isError" class="rounded-2xl border border-rose-300/20 bg-rose-500/10 p-6 text-center text-xs text-rose-200">
            <p class="font-bold m-0">{{ t('Unable to load source data') }}</p>
            <p class="mt-1 text-white/50">{{ sourcesError?.message }}</p>
            <button type="button" class="mt-3 rounded-lg bg-white px-3 py-1.5 text-[10px] font-bold text-slate-950" @click="refetch()">{{ t('Retry') }}</button>
          </div>

          <div v-else-if="!paginatedSources.length" class="py-6">
            <EmptyState
              :title="t('No Matching Records')"
              :message="t('No quantum sources found matching the current search parameters.')"
            />
          </div>

          <div v-else class="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-4">
            <a
              v-for="item in cardSources"
              :key="item.id"
              :href="item.url"
              target="_blank"
              rel="noopener noreferrer"
              class="group flex flex-col overflow-hidden rounded-2xl border border-white/[0.06] bg-black/30 no-underline transition duration-300 hover:-translate-y-0.5 hover:border-cyan-300/30 hover:bg-black/40"
            >
              <div class="relative aspect-video overflow-hidden">
                <img
                  v-if="item.thumb"
                  :src="item.thumb ?? ''"
                  :alt="item.title || t('Source thumbnail')"
                  loading="lazy"
                  decoding="async"
                  class="h-full w-full object-cover transition duration-500 group-hover:scale-105"
                  @error="markThumbFailed(item.id)"
                />
                <div v-else class="flex h-full w-full items-center justify-center" :class="coverTint(item.platform)">
                  <svg v-if="item.platform === 'youtube'" class="h-14 w-14 text-white/60" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="10" />
                    <path d="m10 8 6 4-6 4V8Z" />
                  </svg>
                  <svg v-else-if="item.platform === 'gdelt'" class="h-14 w-14 text-white/60" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 1-2 2Zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2" />
                    <path d="M18 14h-8M15 18h-5M10 6h8v4h-8V6Z" />
                  </svg>
                  <svg v-else-if="item.platform === 'web'" class="h-14 w-14 text-white/60" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="10" />
                    <line x1="2" y1="12" x2="22" y2="12" />
                    <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" />
                  </svg>
                  <svg v-else class="h-14 w-14 text-white/60" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
                    <path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z" />
                  </svg>
                </div>
                <div class="pointer-events-none absolute inset-0 bg-gradient-to-t from-black/80 via-black/5 to-transparent" />
                <span class="absolute left-3 top-3 rounded-full border border-white/10 bg-black/60 px-2 py-0.5 font-mono text-[10px] font-bold text-white backdrop-blur">
                  {{ platformName(item.platform) }}
                </span>
                <span class="absolute right-3 top-3 rounded-full border border-white/10 bg-black/60 px-2 py-0.5 font-mono text-[10px] text-white backdrop-blur">
                  {{ item.published_year }}
                </span>
              </div>

              <div class="flex flex-1 flex-col gap-2 p-4">
                <h3 class="m-0 line-clamp-2 text-sm font-bold leading-snug text-white transition group-hover:text-cyan-300">
                  {{ item.title || item.url }}
                </h3>
                <p class="m-0 truncate font-mono text-[11px] text-sky-200/60">
                  {{ item.channel_title || hostOf(item.url) }}
                  <span v-if="item.view_count"> · {{ formatCount(item.view_count) }} {{ t('views') }}</span>
                </p>
                <div class="mt-auto flex items-center justify-between border-t border-white/[0.06] pt-3">
                  <span class="font-mono text-[11px] text-white/45">#{{ item.id }}</span>
                  <span class="inline-flex items-center gap-1 font-mono text-[11px] font-bold text-cyan-300 transition group-hover:text-white">
                    {{ t('Source') }} <span aria-hidden="true">↗</span>
                  </span>
                </div>
              </div>
            </a>
          </div>
        </div>

        <!-- Pagination Footer -->
        <div class="p-4 bg-black/35 flex flex-wrap items-center justify-between gap-3 text-xs font-mono text-white/80">
          <div>
            {{ t('Showing') }} {{ shownStart }}–{{ shownEnd }} {{ t('of') }} {{ filteredSources.length }} {{ t('records') }}
          </div>

          <div class="flex flex-wrap items-center gap-2">
            <button
              :disabled="currentPage === 1"
              @click="currentPage--"
              class="px-3.5 py-1.5 rounded-xl bg-white/10 hover:bg-white/20 text-white disabled:opacity-30 disabled:cursor-not-allowed transition cursor-pointer"
            >
              ← {{ t('Previous') }}
            </button>
            <span class="text-white/90">{{ t('Page') }} {{ currentPage }} / {{ totalPages }}</span>
            <button
              :disabled="currentPage >= totalPages"
              @click="currentPage++"
              class="px-3.5 py-1.5 rounded-xl bg-white/10 hover:bg-white/20 text-white disabled:opacity-30 disabled:cursor-not-allowed transition cursor-pointer"
            >
              {{ t('Next') }} →
            </button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
