<script setup lang="ts">
import { computed, ref } from 'vue'
import { useIntersectionObserver } from '@vueuse/core'
import { useCorpusCoverageQuery, useStatsSummaryQuery, type AnalyticsFilters } from '@/api/queries'
import DonutBreakdown from '@/components/analytics/DonutBreakdown.vue'
import AppTooltip from '@/components/ui/AppTooltip.vue'
import CountTo from '@/components/ui/CountTo.vue'
import {
  Select as UiSelect,
  SelectContent as UiSelectContent,
  SelectItem as UiSelectItem,
  SelectTrigger as UiSelectTrigger,
  SelectValue as UiSelectValue,
} from '@/components/ui/select'
import { t } from '@/composables/useLocale'

const selectedYear = ref('all')
const selectedPlatform = ref('all')
const selectedDomain = ref('all')
const activeFilters = computed<AnalyticsFilters>(() => ({
  year: selectedYear.value === 'all' ? undefined : selectedYear.value,
  platform: selectedPlatform.value === 'all' ? undefined : selectedPlatform.value,
  domain: selectedDomain.value === 'all' ? undefined : selectedDomain.value,
}))
const { data: stats, isPending: statsPending, isError: statsError, refetch: refetchStats } = useStatsSummaryQuery(activeFilters)
const { data: coverage, isPending: coveragePending, isError: coverageError, refetch: refetchCoverage } = useCorpusCoverageQuery(activeFilters)

const platformNames: Record<string, string> = {
  youtube: 'YouTube',
  gdelt: 'GDELT',
  manual_seed: 'Manual Seed',
  web: 'Web',
}

const platformColors: Record<string, string> = {
  youtube: '#ffffff',
  gdelt: '#67e8f9',
  manual_seed: '#329dd2',
  web: '#a5b4fc',
}

const platformKeys = ['youtube', 'gdelt', 'manual_seed', 'web'] as const

function segmentWidth(count: number, total: number): string {
  return total > 0 ? `${(count / total) * 100}%` : '0%'
}

function platformName(platform: string) {
  return t(platformNames[platform] ?? platform)
}

const years = computed(() => coverage.value?.years ?? [])

const yearRows = computed(() =>
  years.value.map((year) => {
    const counts = coverage.value?.by_year?.[year] ?? {}
    const segments = platformKeys
      .map((key) => ({
        key,
        count: counts[key] ?? 0,
        color: platformColors[key],
      }))
      .filter((segment) => segment.count > 0)
    return {
      year,
      total: segments.reduce((sum, segment) => sum + segment.count, 0),
      segments,
    }
  })
)

const maxYearTotal = computed(() =>
  Math.max(...yearRows.value.map((row) => row.total), 1)
)

function barHeight(total: number) {
  return `${Math.max((total / maxYearTotal.value) * 100, 2)}%`
}

function segmentHeight(count: number, total: number) {
  return `${total > 0 ? (count / total) * 100 : 0}%`
}

const peakYear = computed(() => {
  if (selectedYear.value !== 'all') {
    const row = yearRows.value.find((item) => item.year === selectedYear.value)
    return row ? { year: row.year, total: row.total } : { year: '—', total: 0 }
  }
  let best = { year: '—', total: 0 }
  for (const row of yearRows.value) {
    if (row.total > best.total) best = { year: row.year, total: row.total }
  }
  return best
})

const coveredYears = computed(() =>
  selectedYear.value === 'all' ? years.value.length : yearRows.value.some((row) => row.year === selectedYear.value) ? 1 : 0
)

const totalDocuments = computed(() => coverage.value?.total ?? stats.value?.total_sources ?? 0)

const domains = computed(() => coverage.value?.by_domain ?? [])
const maxDomain = computed(() => Math.max(...domains.value.map((item) => item.count), 1))

const domainColors = ['#ffffff', '#67e8f9', '#329dd2', '#a5b4fc', '#f0abfc', '#fbbf24', '#34d399']

function domainColor(index: number) {
  return domainColors[index % domainColors.length]
}

const domainLabels: Record<string, string> = {
  quantum_computing: 'Quantum Computing',
  quantum_fundamentals: 'Quantum Fundamentals',
  quantum_education: 'Quantum Education',
  quantum_policy_industry: 'Policy & Industry',
  quantum_communication: 'Communication & Security',
  quantum_materials: 'Materials & Sensing',
  quantum_sensing: 'Quantum Sensing',
}

const domainOptions = Object.keys(domainLabels)
const platformFilterOptions = computed(() => [
  { value: 'all', label: t('All platforms') },
  { value: 'youtube', label: 'YouTube' },
  { value: 'gdelt', label: 'GDELT' },
  { value: 'manual_seed', label: t('Manual Seed') },
  { value: 'web', label: t('Web') },
])
const domainFilterOptions = computed(() => [
  { value: 'all', label: t('All domains') },
  ...domainOptions.map((domain) => ({ value: domain, label: domainName(domain) })),
])
const yearFilterOptions = computed(() => [
  { value: 'all', label: t('All years') },
  ...[...years.value].reverse().map((year) => ({ value: year, label: year })),
])

function domainName(domain: string) {
  return t(domainLabels[domain] ?? domain.replace(/_/g, ' '))
}

function domainWidth(count: number) {
  return `${Math.max((count / maxDomain.value) * 100, 2)}%`
}

const platformItems = computed(() =>
  (stats.value?.platforms ?? []).map((item) => ({
    key: item.platform,
    name: platformName(item.platform),
    count: item.count,
    color: platformColors[item.platform.toLowerCase()] ?? '#94a3b8',
  }))
)

const platformTotal = computed(() =>
  platformItems.value.reduce((sum, item) => sum + item.count, 0)
)

const analyticsLoading = computed(() => statsPending.value || coveragePending.value)
const analyticsError = computed(() => statsError.value || coverageError.value)

const domainSection = ref<HTMLElement | null>(null)
const annualSection = ref<HTMLElement | null>(null)

const domainInView = ref(false)
const annualInView = ref(false)

const { stop: stopDomainObserver } = useIntersectionObserver(
  domainSection,
  (entries) => {
    if (!entries.some((entry) => entry.isIntersecting)) return
    domainInView.value = true
    stopDomainObserver()
  },
  { threshold: 0.2 }
)

const { stop: stopAnnualObserver } = useIntersectionObserver(
  annualSection,
  (entries) => {
    if (!entries.some((entry) => entry.isIntersecting)) return
    annualInView.value = true
    stopAnnualObserver()
  },
  { threshold: 0.2 }
)

function retryAnalytics() {
  void refetchStats()
  void refetchCoverage()
}
</script>

<template>
  <div class="w-full select-none pb-6 space-y-5" :aria-busy="analyticsLoading">
    <!-- Header -->
    <div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
      <div class="flex w-full items-center justify-between gap-2.5 lg:w-auto lg:justify-start">
        <h1 class="min-w-0 text-2xl sm:text-[36px] font-black text-white tracking-tight leading-tight m-0 uppercase">
          {{ t('Trends & Analytics') }}
        </h1>
        <AppTooltip :content="t('Quantitative Analytics & Trend Modeling')" side="bottom">
          <button
            type="button"
            class="inline-flex h-8 w-8 items-center justify-center rounded-full text-white/50 hover:text-white hover:bg-white/10 transition cursor-pointer flex-shrink-0"
            :aria-label="t('More information')"
          >
            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="10" />
              <path d="M12 16v-4" />
              <path d="M12 8h.01" />
            </svg>
          </button>
        </AppTooltip>
      </div>

      <div class="grid w-full grid-cols-2 gap-2 lg:flex lg:w-auto lg:flex-nowrap lg:items-center">
        <UiSelect v-model="selectedPlatform">
          <UiSelectTrigger class="w-full bg-[#329dd2] hover:bg-[#2a8ab5] lg:w-[178px]">
            <UiSelectValue />
          </UiSelectTrigger>
          <UiSelectContent>
            <UiSelectItem v-for="option in platformFilterOptions" :key="option.value" :value="option.value">
              {{ option.label }}
            </UiSelectItem>
          </UiSelectContent>
        </UiSelect>

        <UiSelect v-model="selectedYear">
          <UiSelectTrigger class="w-full bg-[#329dd2] hover:bg-[#2a8ab5] lg:w-[130px]">
            <UiSelectValue />
          </UiSelectTrigger>
          <UiSelectContent>
            <UiSelectItem v-for="option in yearFilterOptions" :key="option.value" :value="option.value">
              {{ option.label }}
            </UiSelectItem>
          </UiSelectContent>
        </UiSelect>

        <div class="col-span-2 lg:col-span-1">
          <UiSelect v-model="selectedDomain">
            <UiSelectTrigger class="w-full bg-[#329dd2] hover:bg-[#2a8ab5] lg:w-[178px]">
              <UiSelectValue />
            </UiSelectTrigger>
            <UiSelectContent>
              <UiSelectItem v-for="option in domainFilterOptions" :key="option.value" :value="option.value">
                {{ option.label }}
              </UiSelectItem>
            </UiSelectContent>
          </UiSelect>
        </div>
      </div>
    </div>

    <div
      v-if="analyticsError"
      class="flex flex-col gap-3 rounded-2xl border border-rose-300/20 bg-rose-500/10 px-4 py-3 text-xs text-rose-200 sm:flex-row sm:items-center sm:justify-between"
    >
      <span>{{ t('Unable to load analytics data') }}</span>
      <button type="button" class="w-fit rounded-lg bg-[#ff4c2b] px-3 py-1.5 text-[10px] font-bold text-white hover:bg-[#e84324]" @click="retryAnalytics">
        {{ t('Retry') }}
      </button>
    </div>

    <!-- KPI cards + domain coverage -->
    <div class="grid grid-cols-1 xl:grid-cols-[1.05fr_0.95fr] gap-4 items-stretch">
      <div class="grid grid-cols-2 gap-3 sm:gap-4">
      <article class="rounded-[26px] p-4 sm:p-5 text-white flex flex-col items-center justify-center gap-2 text-center min-h-[116px]" style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);">
        <strong class="text-4xl sm:text-5xl font-black font-mono text-white leading-none"><CountTo :value="totalDocuments" :duration="1100" /></strong>
        <span class="text-[10px] font-mono uppercase tracking-[0.12em] text-sky-200/80">{{ t('Corpus Sources') }}</span>
      </article>
      <article class="rounded-[26px] p-4 sm:p-5 text-white flex flex-col items-center justify-center gap-2 text-center min-h-[116px]" style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);">
        <strong class="text-4xl sm:text-5xl font-black font-mono text-white leading-none"><CountTo :value="stats?.total_triplets ?? 0" format :duration="1200" /></strong>
        <span class="text-[10px] font-mono uppercase tracking-[0.12em] text-sky-200/80">{{ t('SRO Triplets') }}</span>
      </article>
      <article class="rounded-[26px] p-4 sm:p-5 text-white flex flex-col items-center justify-center gap-2 text-center min-h-[116px]" style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);">
        <strong class="text-4xl sm:text-5xl font-black font-mono text-white leading-none"><CountTo :value="coveredYears" :duration="900" /></strong>
        <span class="text-[10px] font-mono uppercase tracking-[0.12em] text-sky-200/80">{{ t('Years Covered') }}</span>
      </article>
      <article class="rounded-[26px] p-4 sm:p-5 text-white flex flex-col items-center justify-center gap-2 text-center min-h-[116px]" style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);">
        <strong class="text-4xl sm:text-5xl font-black font-mono text-white leading-none">{{ peakYear.year }}</strong>
        <span class="text-[10px] font-mono uppercase tracking-[0.12em] text-sky-200/80">{{ t('Peak Year') }}</span>
      </article>
      </div>

      <!-- Domain coverage -->
      <section
        ref="domainSection"
        class="rounded-[30px] p-5 sm:p-6 text-white space-y-5 xl:h-full"
        style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);"
      >
        <header class="flex items-start justify-between gap-3">
          <div>
            <h2 class="text-lg sm:text-xl font-bold tracking-tight m-0">{{ t('Quantum Domain Coverage') }}</h2>
            <p class="text-[11px] text-sky-200/80 m-0 mt-1">{{ t('Document distribution across quantum domains in the current corpus.') }}</p>
          </div>
        </header>

        <div v-if="analyticsLoading" class="space-y-3" aria-hidden="true">
          <div v-for="i in 7" :key="i" class="space-y-1.5">
            <div class="flex items-center justify-between gap-3">
              <span class="h-4 w-32 rounded bg-white/10 animate-pulse" />
              <span class="h-4 w-6 rounded bg-white/10 animate-pulse" />
            </div>
            <div class="h-2 rounded-full bg-white/10" />
          </div>
        </div>
        <div v-else class="space-y-3">
          <div v-for="(domain, index) in domains" :key="domain.domain" class="space-y-1.5">
            <div class="flex items-center justify-between gap-3 text-xs">
              <span class="font-semibold text-white/90 truncate">{{ domainName(domain.domain) }}</span>
              <span class="font-mono text-sky-200/80 flex-shrink-0"><CountTo :value="domain.count" :duration="900" /></span>
            </div>
            <div class="h-2 rounded-full bg-black/30 overflow-hidden">
              <div
                class="h-full rounded-full transition-all duration-700"
                :class="{ 'domain-grow': domainInView }"
                :style="{ width: domainWidth(domain.count), background: domainColor(index) }"
              />
            </div>
          </div>
        </div>
      </section>


    </div>

    <!-- Annual volume + platform split -->
    <div class="grid grid-cols-1 xl:grid-cols-[1.6fr_0.9fr] gap-4">
      <!-- Annual volume -->
      <section
        ref="annualSection"
        class="rounded-[30px] p-5 sm:p-6 text-white space-y-5"
        style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);"
      >
        <header class="flex items-start justify-between gap-3">
          <div>
            <h2 class="text-lg sm:text-xl font-bold tracking-tight m-0">{{ t('Annual Corpus Volume') }}</h2>
            <p class="text-[11px] text-sky-200/80 m-0 mt-1">{{ t('By platform, 2016–2026') }}</p>
          </div>
          <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white whitespace-nowrap">
            {{ t('Yearly') }}
          </span>
        </header>

        <div class="space-y-2 sm:hidden">
          <div
            v-for="row in yearRows"
            :key="`mobile-${row.year}`"
            class="grid grid-cols-[42px_minmax(0,1fr)_32px] items-center gap-2 transition-opacity" :class="selectedYear !== 'all' && row.year !== selectedYear ? 'opacity-30' : ''"
          >
            <span class="font-mono text-[9px] text-sky-200/70">{{ row.year }}</span>
            <div class="flex h-2.5 overflow-hidden rounded-full bg-white/10">
              <div
                v-for="segment in row.segments"
                :key="segment.key"
                :style="{ width: segmentWidth(segment.count, row.total), background: segment.color }"
                :title="`${platformName(segment.key)}: ${segment.count}`"
              />
            </div>
            <span class="text-right font-mono text-[9px] text-sky-200/80">{{ row.total }}</span>
          </div>
        </div>

        <div class="hidden items-end gap-2 pt-2 sm:flex sm:h-[220px]">
          <div v-for="row in yearRows" :key="row.year" class="flex-1 min-w-0 h-full flex flex-col items-center gap-2 transition-opacity" :class="selectedYear !== 'all' && row.year !== selectedYear ? 'opacity-30' : ''">
            <div class="w-full flex-1 flex items-end">
              <div
                class="w-full rounded-t-md overflow-hidden flex flex-col-reverse bg-white/10"
                :class="{ 'bar-grow': annualInView }"
                :style="{ height: barHeight(row.total) }"
              >
                <div
                  v-for="segment in row.segments"
                  :key="segment.key"
                  :style="{ height: segmentHeight(segment.count, row.total), background: segment.color }"
                  :title="`${platformName(segment.key)}: ${segment.count}`"
                />
              </div>
            </div>
            <span class="text-[10px] font-mono text-sky-200/70">{{ row.year }}</span>
          </div>
        </div>

        <div class="flex flex-wrap items-center gap-x-4 gap-y-2">
          <span v-for="key in platformKeys" :key="key" class="inline-flex items-center gap-1.5 text-[10px] font-mono text-sky-200/80">
            <span class="w-2 h-2 rounded-full" :style="{ background: platformColors[key] }" />
            {{ platformName(key) }}
          </span>
        </div>
      </section>

      <!-- Platform split -->
      <section
        class="rounded-[30px] p-5 sm:p-6 text-white space-y-4"
        style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);"
      >
        <header>
          <h2 class="text-lg sm:text-xl font-bold tracking-tight m-0">{{ t('Platforms & Corpus Signals') }}</h2>
          <p class="text-[11px] text-sky-200/80 m-0 mt-1">{{ t('Corpus volume segmented by channel of origin') }}</p>
        </header>
        <DonutBreakdown :items="platformItems" :total="platformTotal" :unit="t('sources')" />
      </section>
    </div>


  </div>
</template>

<style scoped>
.bar-grow {
  transform-origin: bottom;
  animation: bar-grow 700ms cubic-bezier(0.2, 0, 0, 1) both;
}

@keyframes bar-grow {
  from {
    transform: scaleY(0);
    opacity: 0.35;
  }
  to {
    transform: scaleY(1);
    opacity: 1;
  }
}

.domain-grow {
  transform-origin: left;
  animation: domain-grow 700ms cubic-bezier(0.2, 0, 0, 1) both;
}

@keyframes domain-grow {
  from {
    transform: scaleX(0);
  }
  to {
    transform: scaleX(1);
  }
}
</style>
