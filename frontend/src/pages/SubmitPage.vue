<script setup lang="ts">
import { ref } from 'vue'
import { useQueryClient } from '@tanstack/vue-query'
import { useAuthUserQuery, useSubmitDataMutation } from '@/api/queries'
import Alert from '@/components/ui/Alert.vue'
import AuthMenu from '@/components/auth/AuthMenu.vue'
import AppTooltip from '@/components/ui/AppTooltip.vue'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { t } from '@/composables/useLocale'
import { THAI_PROVINCES } from '@/lib/provinces'

const { data: authUser } = useAuthUserQuery()
const queryClient = useQueryClient()
const mutation = useSubmitDataMutation()

const title = ref('')
const url = ref('')
const category = ref('')
const handle = ref('')
const province = ref('none')
const isSubmitting = ref(false)
const statusMessage = ref<{ type: 'success' | 'error'; text: string } | null>(null)

const categories = [
  'Quantum Computing',
  'Quantum Sensing',
  'Quantum Communications',
  'Quantum Information',
  'Quantum Hardware & Materials',
  'Quantum Education & Policy',
]

function submitResearch() {
  if (!title.value || !url.value || !category.value) return
  isSubmitting.value = true
  statusMessage.value = null

  mutation.mutate(
    {
      title: title.value.trim(),
      url: url.value.trim(),
      category: category.value,
      handle: handle.value.trim() || undefined,
      province_code: province.value === 'none' ? null : province.value,
    },
    {
      onSuccess: () => {
        statusMessage.value = {
          type: 'success',
          text: t('Research source submitted successfully! An admin will review it before it is published.'),
        }
        title.value = ''
        url.value = ''
        category.value = ''
        handle.value = ''
        province.value = 'none'
        isSubmitting.value = false
        queryClient.invalidateQueries({ queryKey: ['submitted-data', 'mine'] })
      },
      onError: (err: any) => {
        statusMessage.value = {
          type: 'error',
          text: err?.message || t('Failed to submit source to pipeline. Please verify URL formatting.'),
        }
        isSubmitting.value = false
      },
    }
  )
}
</script>

<template>
  <div class="w-full select-none pb-6 space-y-5">
    <!-- Header -->
    <div class="flex flex-col sm:flex-row sm:items-end justify-between gap-3">
      <div class="flex items-center gap-2.5">
        <h1 class="text-2xl sm:text-[36px] font-black text-white tracking-tight leading-tight m-0 uppercase">
          {{ t('Research Input') }}
        </h1>
        <AppTooltip
          :content="t('Community Research Ingest — Contribute articles, lab papers, university announcements, or lecture videos to expand the Thai quantum knowledge graph.')"
          side="bottom"
        >
          <button
            type="button"
            class="inline-flex h-8 w-8 items-center justify-center rounded-full text-white/50 hover:text-white hover:bg-white/10 transition cursor-pointer"
            :aria-label="t('More information about submitting research')"
          >
            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="10" />
              <path d="M12 16v-4" />
              <path d="M12 8h.01" />
            </svg>
          </button>
        </AppTooltip>
      </div>
    </div>

    <!-- Signed out -->
    <div
      v-if="!authUser"
      class="rounded-[30px] p-6 sm:p-10 text-white flex flex-col sm:flex-row items-start sm:items-center justify-between gap-5"
      style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);"
    >
      <div class="flex items-center gap-4">
        <div class="w-14 h-14 rounded-2xl bg-black/40 flex items-center justify-center flex-shrink-0">
          <svg class="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <rect x="4" y="10" width="16" height="11" rx="2" />
            <path d="M8 10V7a4 4 0 0 1 8 0v3" />
          </svg>
        </div>
        <div>
          <h2 class="text-lg sm:text-xl font-bold m-0">{{ t('Sign in to submit research.') }}</h2>
          <p class="text-xs sm:text-sm text-sky-200/80 m-0 mt-1">{{ t('Sign in to unlock submission features and analyst privileges.') }}</p>
        </div>
      </div>
      <AuthMenu variant="cta" />
    </div>

    <div v-else class="grid grid-cols-1 lg:grid-cols-[1.15fr_0.85fr] gap-5 items-stretch">
      <!-- Form -->
      <form
        @submit.prevent="submitResearch"
        class="rounded-[30px] p-5 sm:p-7 text-white space-y-4"
        style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);"
      >
        <header class="flex items-center justify-between gap-3">
          <div class="flex items-center gap-2.5 min-w-0">
            <svg class="w-5 h-5 text-white flex-none" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
              <polyline points="17 8 12 3 7 8" />
              <line x1="12" y1="3" x2="12" y2="15" />
            </svg>
            <h2 class="text-base sm:text-lg font-bold tracking-tight m-0 truncate">{{ t('Submit Quantum Research') }}</h2>
          </div>
          <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white whitespace-nowrap">
            {{ t('AI Pipeline') }}
          </span>
        </header>

        <div class="space-y-1.5">
          <label class="text-[10px] font-mono uppercase tracking-[0.12em] text-white/90 font-bold block min-h-[2rem]">
            {{ t('Research Title') }} <span class="text-cyan-300">*</span>
          </label>
          <input
            v-model="title"
            type="text"
            required
            :placeholder="t('e.g. Chulalongkorn launches quantum computing research center')"
            class="h-11 w-full min-w-0 px-3.5 rounded-xl bg-white/[0.1] hover:bg-white/[0.14] focus:bg-white/[0.18] text-sm text-white placeholder-white/45 focus:outline-none focus:ring-2 focus:ring-cyan-300/40 transition"
          >
        </div>

        <div class="space-y-1.5">
          <label class="text-[10px] font-mono uppercase tracking-[0.12em] text-white/90 font-bold block min-h-[2rem]">
            {{ t('Source URL') }} <span class="text-cyan-300">*</span>
          </label>
          <input
            v-model="url"
            type="url"
            required
            :placeholder="t('https://chula.ac.th/news/... or https://youtube.com/watch?v=...')"
            class="h-11 w-full min-w-0 px-3.5 rounded-xl bg-white/[0.1] hover:bg-white/[0.14] focus:bg-white/[0.18] text-sm text-white placeholder-white/45 focus:outline-none focus:ring-2 focus:ring-cyan-300/40 transition"
          >
        </div>

        <div class="grid grid-cols-1 sm:grid-cols-[repeat(3,minmax(0,1fr))] sm:grid-rows-[auto_auto] gap-3">
          <div class="grid min-w-0 gap-1.5 sm:row-span-2 sm:grid-rows-subgrid">
            <label class="text-[10px] font-mono uppercase tracking-[0.12em] text-white/90 font-bold block">
              {{ t('Category') }} <span class="text-cyan-300">*</span>
            </label>
            <Select v-model="category">
              <SelectTrigger class="w-full min-w-0 h-11 text-sm overflow-hidden [&>span]:min-w-0 [&>span]:truncate">
                <SelectValue :placeholder="t('Select a category')" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem v-for="item in categories" :key="item" :value="item">
                  {{ t(item) }}
                </SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div class="grid min-w-0 gap-1.5 sm:row-span-2 sm:grid-rows-subgrid">
            <label class="text-[10px] font-mono uppercase tracking-[0.12em] text-white/90 font-bold block">
              {{ t('Province (Optional)') }}
            </label>
            <Select v-model="province">
              <SelectTrigger class="w-full min-w-0 h-11 text-sm overflow-hidden [&>span]:min-w-0 [&>span]:truncate">
                <SelectValue :placeholder="t('Select a province')" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="none">{{ t('Not specified (country-level)') }}</SelectItem>
                <SelectItem v-for="item in THAI_PROVINCES" :key="item" :value="item">
                  {{ item }}
                </SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div class="grid min-w-0 gap-1.5 sm:row-span-2 sm:grid-rows-subgrid">
            <label class="text-[10px] font-mono uppercase tracking-[0.12em] text-white/90 font-bold block">
              {{ t('Contributor Handle (Optional)') }}
            </label>
            <input
              v-model="handle"
              type="text"
              :placeholder="t('@researcher_name or institution')"
              class="h-11 w-full min-w-0 px-3.5 rounded-xl bg-white/[0.1] hover:bg-white/[0.14] focus:bg-white/[0.18] text-sm text-white placeholder-white/45 focus:outline-none focus:ring-2 focus:ring-cyan-300/40 transition"
            >
          </div>
        </div>

        <div v-if="statusMessage">
          <Alert :variant="statusMessage.type === 'success' ? 'success' : 'danger'">
            {{ statusMessage.text }}
          </Alert>
        </div>

        <button
          type="submit"
          :disabled="isSubmitting"
          class="w-full py-3 rounded-2xl bg-[#ff4c2b] hover:bg-[#e84324] text-white font-bold font-mono uppercase tracking-[0.12em] text-xs transition disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 cursor-pointer"
        >
          <span v-if="isSubmitting" class="w-3.5 h-3.5 rounded-full bg-white/50 animate-ping" />
          <span>{{ isSubmitting ? t('Evaluating & Ingesting...') : t('Submit to Atlas Pipeline') }}</span>
        </button>
      </form>

      <!-- How it works -->
      <aside
        class="rounded-[30px] p-5 sm:p-7 text-white flex flex-col justify-between gap-5"
        style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.35) 55%, rgba(50, 157, 210, 0) 100%);"
      >
        <div class="space-y-4">
          <header class="flex items-center gap-2.5">
            <svg class="w-5 h-5 text-white flex-none" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
              <circle cx="12" cy="12" r="10" />
              <path d="M12 16v-4" />
              <path d="M12 8h.01" />
            </svg>
            <h2 class="text-base sm:text-lg font-bold m-0">{{ t('How it works') }}</h2>
          </header>

          <ol class="space-y-3 m-0 p-0 list-none">
            <li class="flex gap-3">
              <span class="w-6 h-6 rounded-full bg-black/55 border border-white/10 flex items-center justify-center text-[10px] font-mono font-bold flex-shrink-0">1</span>
              <div>
                <p class="text-sm font-bold m-0">{{ t('Submit a source') }}</p>
                <p class="text-[11px] text-sky-200/80 m-0 mt-0.5">{{ t('Add the title, URL, and quantum category of the research or article.') }}</p>
              </div>
            </li>
            <li class="flex gap-3">
              <span class="w-6 h-6 rounded-full bg-black/55 border border-white/10 flex items-center justify-center text-[10px] font-mono font-bold flex-shrink-0">2</span>
              <div>
                <p class="text-sm font-bold m-0">{{ t('AI extracts knowledge') }}</p>
                <p class="text-[11px] text-sky-200/80 m-0 mt-0.5">{{ t('The pipeline extracts SRO triplets and geolocates the source automatically.') }}</p>
              </div>
            </li>
            <li class="flex gap-3">
              <span class="w-6 h-6 rounded-full bg-black/55 border border-white/10 flex items-center justify-center text-[10px] font-mono font-bold flex-shrink-0">3</span>
              <div>
                <p class="text-sm font-bold m-0">{{ t('Admin review') }}</p>
                <p class="text-[11px] text-sky-200/80 m-0 mt-0.5">{{ t('An admin verifies the submission before it is published to the Atlas.') }}</p>
              </div>
            </li>
          </ol>
        </div>

        <div class="rounded-2xl bg-black/30 p-3.5 text-[11px] font-mono text-sky-200/80 space-y-1">
          <div class="flex items-center justify-between gap-3">
            <span>{{ t('Model: Claude 3.5 Sonnet') }}</span>
            <span class="text-white">{{ t('Enabled') }}</span>
          </div>
          <div class="flex items-center justify-between gap-3">
            <span>{{ t('Provenance Integrity') }}</span>
            <span class="text-white">{{ t('Enabled') }}</span>
          </div>
        </div>
      </aside>
    </div>
  </div>
</template>
