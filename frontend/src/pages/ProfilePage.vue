<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useAuthUserQuery, useMySubmissionsQuery, type MySubmission } from '@/api/queries'
import AppTooltip from '@/components/ui/AppTooltip.vue'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { t } from '@/composables/useLocale'

const { data: authUser } = useAuthUserQuery()
const { data: mySubmissions } = useMySubmissionsQuery(computed(() => Boolean(authUser.value)))

interface ProfileData {
  displayName: string
  role: string
  bio: string
  email: string
  organization: string
  initials: string
  focus: string
  citation: string
}

const guestProfile: ProfileData = {
  displayName: 'Guest Researcher',
  role: 'Not signed in',
  bio: '',
  email: '',
  organization: '—',
  initials: '?',
  focus: '—',
  citation: '—',
}

const defaultProfile: ProfileData = {
  displayName: 'Researcher',
  role: 'Researcher',
  bio: '',
  email: '',
  organization: '',
  initials: 'SQ',
  focus: '',
  citation: 'IEEE / BibTeX Standard',
}

const profile = ref<ProfileData>({ ...guestProfile })

function hydrateProfile() {
  const user = authUser.value
  if (!user) {
    profile.value = { ...guestProfile }
    return
  }

  let saved: Partial<ProfileData> = {}
  const raw = localStorage.getItem('sq_profile_data')
  if (raw) {
    try {
      saved = JSON.parse(raw) as Partial<ProfileData>
    } catch {
      saved = {}
    }
  }

  const name = saved.displayName || user.display_name || user.email?.split('@')[0] || 'Researcher'
  profile.value = {
    ...defaultProfile,
    displayName: name,
    email: saved.email || user.email || '',
    role: saved.role || (user.role === 'admin' ? 'Administrator' : user.role || 'Researcher'),
    bio: saved.bio || '',
    initials: saved.initials || name.slice(0, 2).toUpperCase() || 'SQ',
    organization: saved.organization || '',
    focus: saved.focus || '',
    citation: saved.citation || defaultProfile.citation,
  }
}

watch(authUser, hydrateProfile, { immediate: true })

const activeTab = ref<'overview' | 'apis'>('overview')

const personalStats = computed(() => {
  const items = mySubmissions.value ?? []
  let pending = 0
  let approved = 0
  let rejected = 0
  for (const item of items) {
    if (item.status === 'approved') approved += 1
    else if (item.status === 'rejected') rejected += 1
    else pending += 1
  }
  return { total: items.length, pending, approved, rejected }
})

const submitCards = computed(() => [
  {
    label: 'My Submissions',
    hint: 'Total submissions from this account',
    value: personalStats.value.total,
  },
  {
    label: 'Pending Review',
    hint: 'Waiting for admin review',
    value: personalStats.value.pending,
  },
  {
    label: 'Approved',
    hint: 'Approved by admin',
    value: personalStats.value.approved,
  },
  {
    label: 'Rejected',
    hint: 'Not accepted',
    value: personalStats.value.rejected,
  },
])

const apiEndpoints = [
  { name: 'Graph API', method: 'GET', path: '/api/graph', desc: 'Subject-relation-object knowledge graph' },
  { name: 'Sources API', method: 'GET', path: '/api/sources', desc: 'Searchable source corpus' },
  { name: 'Stats API', method: 'GET', path: '/api/stats/summary', desc: 'Aggregate corpus metrics' },
  { name: 'Export API', method: 'GET', path: '/api/export/xlsx', desc: 'Download XLSX corpus export' },
]

const isEditOpen = ref(false)
const editForm = ref<ProfileData>({ ...profile.value })
const saveSuccess = ref(false)
const saveError = ref('')

function openEditModal() {
  editForm.value = { ...profile.value }
  isEditOpen.value = true
}

function closeEditModal() {
  isEditOpen.value = false
}

async function saveProfile() {
  if (!authUser.value) return
  profile.value = { ...editForm.value }
  if (!profile.value.initials) {
    profile.value.initials = profile.value.displayName.slice(0, 2).toUpperCase() || 'SQ'
  }
  saveError.value = ''
  try {
    const response = await fetch('/api/profile', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        display_name: profile.value.displayName,
        bio: profile.value.bio || profile.value.role,
        organization: profile.value.organization || null,
        focus: profile.value.focus || null,
        citation: profile.value.citation || null,
      }),
    })
    const json = await response.json().catch(() => null)
    if (!response.ok || !json?.ok) {
      throw new Error(json?.error?.message || t('Failed to save profile'))
    }
  } catch (err: any) {
    saveError.value = err?.message || t('Failed to save profile')
    return
  }

  localStorage.setItem('sq_profile_data', JSON.stringify(profile.value))
  localStorage.setItem('sq_profile_initials', profile.value.initials)
  window.dispatchEvent(new Event('sq_profile_updated'))
  isEditOpen.value = false
  saveSuccess.value = true
  setTimeout(() => {
    saveSuccess.value = false
  }, 3500)
}

const profileIngestions = computed(() => (authUser.value ? mySubmissions.value ?? [] : []))

const platformNames: Record<string, string> = {
  YOUTUBE: 'YouTube',
  WEB: 'Web',
  GDELT: 'GDELT',
  MANUAL_SEED: 'Manual Seed',
}

function submissionPlatform(item: MySubmission): string {
  const raw = String((item.metadata as Record<string, unknown> | undefined)?.platform ?? '').toUpperCase()
  if (raw) return raw
  const url = String(item.source_url ?? '').toLowerCase()
  if (url.includes('youtube.com') || url.includes('youtu.be')) return 'YOUTUBE'
  if (url.includes('gdelt')) return 'GDELT'
  return 'WEB'
}

function platformLabel(platform: string) {
  return t(platformNames[platform] ?? platform)
}

function timeAgo(value?: string): string {
  if (!value) return ''
  const created = new Date(value).getTime()
  if (Number.isNaN(created)) return ''
  const days = Math.max(0, Math.floor((Date.now() - created) / 86_400_000))
  if (days === 0) return t('Today')
  if (days === 1) return t('Yesterday')
  return `${days} ${t('days ago')}`
}
</script>

<template>
  <div class="w-full pb-4 select-none lg:h-full lg:pb-0 lg:flex lg:flex-col">
    <!-- Success toast -->
    <div
      v-if="saveError"
      class="mb-4 rounded-2xl border border-rose-300/20 bg-rose-500/10 p-3.5 font-mono text-xs text-rose-200"
    >
      {{ saveError }}
    </div>

    <div
      v-if="saveSuccess"
      class="mb-4 p-3.5 rounded-2xl bg-emerald-500/15 border border-emerald-400/20 text-emerald-300 font-mono text-xs flex items-center justify-between gap-3"
    >
      <div class="flex items-center gap-2">
        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
        <span class="font-bold">{{ t('Profile and research workspace updated successfully!') }}</span>
      </div>
      <button @click="saveSuccess = false" class="text-white/60 hover:text-white cursor-pointer">✕</button>
    </div>

    <!-- Signed-out notice -->
    <div
      v-if="!authUser"
      class="mb-4 p-3.5 rounded-2xl bg-[#1d4ed8]/20 border border-[#60a5fa]/25 text-sky-100 flex flex-col sm:flex-row sm:items-center justify-between gap-3 text-xs font-mono lg:hidden"
    >
      <span>{{ t('You are not signed in. Sign in from the dashboard to manage your profile.') }}</span>
      <router-link
        to="/dashboard"
        class="px-4 py-2 rounded-xl bg-white text-slate-950 font-bold no-underline whitespace-nowrap text-center"
      >
        {{ t('Go to Dashboard') }}
      </router-link>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1.55fr)_minmax(300px,0.85fr)] gap-4 xl:gap-5 items-start lg:flex-1 lg:min-h-0 lg:items-stretch">
      <!-- ── LEFT: identity + tabs ─────────────────────────────────────── -->
      <section
        class="rounded-[28px] p-4 sm:p-5 xl:p-6 space-y-3 text-white lg:h-full lg:min-h-0 lg:overflow-hidden"
        style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
      >
        <!-- Identity header -->
        <header class="flex items-center gap-4 sm:gap-5">
          <div class="w-14 h-14 sm:w-16 sm:h-16 rounded-full bg-black/40 border-4 border-white/15 flex items-center justify-center text-xl sm:text-2xl font-black font-mono text-cyan-300 flex-shrink-0">
            {{ profile.initials }}
          </div>

          <div class="min-w-0 flex-1">
            <div class="flex items-center gap-2 sm:gap-3">
              <h2 class="text-lg sm:text-2xl xl:text-3xl font-black tracking-tight uppercase m-0 break-words">
                {{ profile.displayName }}
              </h2>
              <AppTooltip v-if="authUser" :content="t('Edit profile')" side="top">
                <button
                  type="button"
                  @click="openEditModal"
                  class="w-8 h-8 sm:w-9 sm:h-9 rounded-full border border-white/[0.14] bg-white/[0.05] text-zinc-300 hover:text-white hover:bg-white/10 flex items-center justify-center transition cursor-pointer flex-shrink-0"
                  :aria-label="t('Edit profile')"
                >
                  <svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M12 20h9" />
                    <path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4Z" />
                  </svg>
                </button>
              </AppTooltip>
              <router-link
                v-if="authUser?.role === 'admin'"
                to="/admin"
                class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[9px] font-mono font-bold uppercase tracking-[0.08em] text-white no-underline hover:bg-black/70 transition flex-shrink-0"
              >
                {{ t('Admin Panel') }}
              </router-link>
            </div>
            <p class="text-xs sm:text-sm font-mono text-cyan-300/90 m-0 mt-1 truncate">
              {{ profile.role }}
            </p>
            <p class="text-xs sm:text-sm text-white/60 m-0 mt-0.5 truncate">
              {{ profile.email || '—' }}
            </p>
          </div>
        </header>

        <!-- Tabs -->
        <div class="flex items-center gap-1 border-b border-white/20">
          <button
            type="button"
            @click="activeTab = 'overview'"
            class="relative px-4 sm:px-5 py-2.5 text-[11px] sm:text-xs font-mono font-bold uppercase tracking-[0.14em] transition cursor-pointer"
            :class="activeTab === 'overview' ? 'text-white' : 'text-white/50 hover:text-white/80'"
          >
            {{ t('Overview') }}
            <span v-if="activeTab === 'overview'" class="absolute -bottom-px left-2 right-2 h-0.5 rounded-full bg-cyan-300" />
          </button>
          <button
            type="button"
            @click="activeTab = 'apis'"
            class="relative px-4 sm:px-5 py-2.5 text-[11px] sm:text-xs font-mono font-bold uppercase tracking-[0.14em] transition cursor-pointer"
            :class="activeTab === 'apis' ? 'text-white' : 'text-white/50 hover:text-white/80'"
          >
            {{ t('APIs') }}
            <span v-if="activeTab === 'apis'" class="absolute -bottom-px left-2 right-2 h-0.5 rounded-full bg-cyan-300" />
          </button>
        </div>

        <!-- ── OVERVIEW TAB ── -->
        <div v-if="activeTab === 'overview'" class="space-y-3">
          <div class="grid grid-cols-2 gap-3 sm:gap-4">
            <article
              v-for="card in submitCards"
              :key="card.label"
              class="rounded-2xl bg-black/30 p-3 min-h-[104px] flex flex-col justify-center gap-2"
            >
              <div class="flex items-center justify-between gap-2">
                <span class="text-[10px] font-mono uppercase tracking-[0.12em] text-sky-200/80 leading-tight">
                  {{ t(card.label) }}
                </span>
                <AppTooltip :content="t(card.hint)" side="top">
                  <button
                    type="button"
                    class="inline-flex h-4 w-4 items-center justify-center rounded-full text-sky-200/60 hover:text-white hover:bg-white/10 transition cursor-pointer flex-shrink-0"
                    :aria-label="t('More information')"
                  >
                    <svg class="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">
                      <circle cx="12" cy="12" r="10" />
                      <path d="M12 16v-4" />
                      <path d="M12 8h.01" />
                    </svg>
                  </button>
                </AppTooltip>
              </div>
              <strong class="text-[clamp(2rem,3.5vw,3rem)] font-black font-mono text-white leading-none">
                {{ card.value }}
              </strong>
            </article>
          </div>
        </div>

        <!-- ── APIS TAB ── -->
        <div v-else class="space-y-5">
          <div class="rounded-2xl border border-white/10 bg-black/30 p-4 sm:p-5 space-y-2">
            <h2 class="text-sm font-bold m-0">{{ t('API Access') }}</h2>
            <p class="text-[11px] leading-relaxed text-sky-200/75 m-0">
              {{ t('Public read endpoints are available below. Authenticated writes use the current local session; personal API tokens are not issued in local mode.') }}
            </p>
          </div>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4">
            <article
              v-for="endpoint in apiEndpoints"
              :key="endpoint.path"
              class="rounded-2xl border border-white/10 bg-black/30 p-4 space-y-2.5"
            >
              <div class="flex items-center justify-between gap-2">
                <span class="text-[11px] font-mono font-bold uppercase tracking-[0.1em] text-white">{{ endpoint.name }}</span>
                <span class="px-2 py-0.5 rounded-md bg-black/55 border border-white/10 text-white font-mono text-[9px] font-bold">
                  {{ endpoint.method }}
                </span>
              </div>
              <code class="block text-[11px] font-mono text-cyan-300 break-all">{{ endpoint.path }}</code>
              <p class="text-[11px] text-sky-200/75 m-0 leading-relaxed">{{ t(endpoint.desc) }}</p>
            </article>
          </div>
        </div>
      </section>

      <!-- ── RIGHT: activity feed ──────────────────────────────────────── -->
      <aside
        class="rounded-[30px] xl:rounded-[34px] p-4 sm:p-5 text-white h-auto lg:h-full min-h-0 flex flex-col justify-between gap-3 overflow-hidden"
        style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
      >
        <header class="flex items-center justify-between flex-shrink-0 text-white pb-2 mb-1">
          <h2 class="flex items-center gap-2.5 text-xl xl:text-[22px] font-bold tracking-tight whitespace-nowrap text-white m-0">
            <svg class="w-5 h-5 xl:w-6 xl:h-6 text-white flex-none" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M2.5 3.5h11v9a1.5 1.5 0 0 1-1.5 1.5h-8A1.5 1.5 0 0 1 2.5 12.5v-9Z" />
              <path d="M2.5 8.5h3.2l1.3 2h2l1.3-2h3.2" />
              <path d="M8 1.5v4.5M6 4l2 2 2-2" />
            </svg>
            <span>{{ t('Recent Ingestions') }}</span>
          </h2>
        </header>

        <!-- Not signed in: no data is exposed -->
        <div v-if="!authUser" class="flex-1 min-h-0 flex flex-col items-center justify-center gap-3 text-center px-4 py-6">
          <div class="w-14 h-14 rounded-2xl bg-white/15 flex items-center justify-center">
            <svg class="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <rect x="4" y="10" width="16" height="11" rx="2" />
              <path d="M8 10V7a4 4 0 0 1 8 0v3" />
            </svg>
          </div>
          <p class="text-sm font-semibold text-white m-0">{{ t('Sign in to view your recent ingestions.') }}</p>
          <p class="text-[11px] text-sky-200/80 m-0 leading-relaxed max-w-[240px]">
            {{ t('Recent ingestions are private and only visible to the account owner.') }}
          </p>
          <router-link
            to="/dashboard"
            class="mt-1 px-4 py-2 rounded-xl bg-white text-blue-900 font-mono text-[11px] font-bold no-underline hover:bg-cyan-100 transition"
          >
            {{ t('Go to Dashboard') }}
          </router-link>
        </div>

        <!-- Signed in: this account's own ingestions only -->
        <div v-else class="flex flex-col gap-2.5 flex-1 min-h-0 justify-between py-0.5 overflow-hidden divide-y divide-white/15">
          <template v-if="profileIngestions.length">
            <div
              v-for="item in profileIngestions.slice(0, 6)"
              :key="item.id"
              class="py-1 flex items-center gap-3.5 flex-shrink-0"
            >
              <AppTooltip :content="platformLabel(submissionPlatform(item))" side="right">
                <div class="w-[48px] h-[48px] rounded-[14px] bg-white/20 text-white flex items-center justify-center flex-shrink-0">
                  <svg v-if="submissionPlatform(item) === 'YOUTUBE'" class="w-[28px] h-[28px] fill-white" viewBox="0 0 24 24">
                    <path d="M23.498 6.186a3.016 3.016 0 0 0-2.122-2.136C19.505 3.545 12 3.545 12 3.545s-7.505 0-9.377.505A3.017 3.017 0 0 0 .502 6.186C0 8.07 0 12 0 12s0 3.93.502 5.814a3.016 3.016 0 0 0 2.122 2.136c1.871.505 9.376.505 9.376.505s7.505 0 9.377-.505a3.015 3.015 0 0 0 2.122-2.136C24 15.93 24 12 24 12s0-3.93-.502-5.814zM9.545 15.568V8.432L15.818 12l-6.273 3.568z"/>
                  </svg>
                  <svg v-else-if="submissionPlatform(item) === 'WEB'" class="w-[28px] h-[28px]" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="10"/>
                    <line x1="2" y1="12" x2="22" y2="12"/>
                    <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>
                  </svg>
                  <svg v-else-if="submissionPlatform(item) === 'GDELT'" class="w-[28px] h-[28px]" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 1-2 2Zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/>
                    <path d="M18 14h-8M15 18h-5M10 6h8v4h-8V6Z"/>
                  </svg>
                  <svg v-else class="w-[28px] h-[28px]" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z"/>
                  </svg>
                </div>
              </AppTooltip>

              <div class="min-w-0 flex-1">
                <p class="text-base xl:text-[19px] font-bold text-white line-clamp-2 leading-snug">
                  {{ item.title || item.source_url || '—' }}
                </p>
                <p class="text-sm xl:text-[16px] font-mono text-sky-200/90 mt-0.5">
                  #{{ item.id }} • {{ timeAgo(item.created_at) }}
                </p>
              </div>
            </div>
          </template>

          <div v-else class="flex-1 flex flex-col items-center justify-center gap-2 text-center px-4">
            <p class="text-sm font-semibold text-white m-0">{{ t('No submissions yet.') }}</p>
            <p class="text-[11px] text-sky-200/80 m-0 leading-relaxed">
              {{ t('Submit a source to see it here.') }}
            </p>
          </div>
        </div>
      </aside>
    </div>

    <!-- ── EDIT PROFILE MODAL ─────────────────────────────────────────── -->
    <Transition
      enter-active-class="transition duration-200 ease-out"
      enter-from-class="opacity-0 scale-95"
      enter-to-class="opacity-100 scale-100"
      leave-active-class="transition duration-150 ease-in"
      leave-from-class="opacity-100 scale-100"
      leave-to-class="opacity-0 scale-95"
    >
      <div
        v-if="isEditOpen"
        class="fixed inset-0 z-[2000] bg-black/70 backdrop-blur-md flex items-center justify-center p-4 overflow-y-auto"
        @click.self="closeEditModal"
      >
        <div class="w-full max-w-2xl rounded-[28px] border border-white/[0.1] p-6 sm:p-8 text-white space-y-6 shadow-2xl my-6"
          style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.70) 55%, rgba(15, 23, 42, 0.98) 100%);">
          <div class="flex items-center justify-between">
            <div class="flex items-center gap-2.5">
              <div class="w-10 h-10 rounded-full bg-white/10 flex items-center justify-center text-cyan-300">
                <svg class="w-4.5 h-4.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M12 20h9" />
                  <path d="M16.5 3.5a2.12 2.12 0 0 1 3 3L7 19l-4 1 1-4Z" />
                </svg>
              </div>
              <div>
                <h3 class="text-lg sm:text-xl font-bold tracking-tight m-0">{{ t('Edit Researcher Profile') }}</h3>
                <span class="text-[11px] font-mono text-cyan-300/80">{{ t('Researcher Profile & Workspace') }}</span>
              </div>
            </div>
            <button
              type="button"
              @click="closeEditModal"
              class="w-9 h-9 rounded-full bg-white/[0.06] hover:bg-white/10 text-white text-base flex items-center justify-center transition cursor-pointer"
            >
              ✕
            </button>
          </div>

          <form @submit.prevent="saveProfile" class="space-y-4 text-sm">
            <div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
              <div class="sm:col-span-2 space-y-1.5">
                <label class="font-mono text-white/60 uppercase text-[10px] tracking-[0.12em] block">{{ t('Display Name') }}</label>
                <input
                  v-model="editForm.displayName"
                  type="text"
                  required
                  :placeholder="t('e.g. Dr. Taechasith')"
                  class="w-full px-3.5 py-2.5 rounded-xl bg-black/40 border border-white/[0.08] text-sm text-white placeholder-white/25 focus:outline-none focus:ring-2 focus:ring-cyan-300/50 transition"
                >
              </div>
              <div class="space-y-1.5">
                <label class="font-mono text-white/60 uppercase text-[10px] tracking-[0.12em] block">{{ t('Initials') }}</label>
                <input
                  v-model="editForm.initials"
                  type="text"
                  maxlength="3"
                  placeholder="SQ"
                  class="w-full px-3.5 py-2.5 rounded-xl bg-black/40 border border-white/[0.08] text-sm text-white placeholder-white/25 font-mono text-center font-bold uppercase focus:outline-none focus:ring-2 focus:ring-cyan-300/50 transition"
                >
              </div>
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div class="space-y-1.5">
                <label class="font-mono text-white/60 uppercase text-[10px] tracking-[0.12em] block">{{ t('Role / Academic Title') }}</label>
                <input
                  v-model="editForm.role"
                  type="text"
                  required
                  :placeholder="t('e.g. Principal Quantum NLP Investigator')"
                  class="w-full px-3.5 py-2.5 rounded-xl bg-black/40 border border-white/[0.08] text-sm text-white placeholder-white/25 focus:outline-none focus:ring-2 focus:ring-cyan-300/50 transition"
                >
              </div>
              <div class="space-y-1.5">
                <label class="font-mono text-white/60 uppercase text-[10px] tracking-[0.12em] block">{{ t('Contact Email') }}</label>
                <input
                  v-model="editForm.email"
                  type="email"
                  required
                  placeholder="analyst@siamquantum.org"
                  class="w-full px-3.5 py-2.5 rounded-xl bg-black/40 border border-white/[0.08] text-sm text-white placeholder-white/25 font-mono focus:outline-none focus:ring-2 focus:ring-cyan-300/50 transition"
                >
              </div>
            </div>

            <div class="space-y-1.5">
              <label class="font-mono text-white/60 uppercase text-[10px] tracking-[0.12em] block">{{ t('Affiliation / Organization') }}</label>
              <input
                v-model="editForm.organization"
                type="text"
                :placeholder="t('e.g. SiamQuantum Consortium / Chulalongkorn Univ.')"
                class="w-full px-3.5 py-2.5 rounded-xl bg-black/40 border border-white/[0.08] text-sm text-white placeholder-white/25 focus:outline-none focus:ring-2 focus:ring-cyan-300/50 transition"
              >
            </div>

            <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div class="space-y-1.5">
                <label class="font-mono text-white/60 uppercase text-[10px] tracking-[0.12em] block">{{ t('Primary Research Focus') }}</label>
                <input
                  v-model="editForm.focus"
                  type="text"
                  :placeholder="t('e.g. QKD & Computing')"
                  class="w-full px-3.5 py-2.5 rounded-xl bg-black/40 border border-white/[0.08] text-sm text-white placeholder-white/25 focus:outline-none focus:ring-2 focus:ring-cyan-300/50 transition"
                >
              </div>
              <div class="space-y-1.5">
                <label class="font-mono text-white/60 uppercase text-[10px] tracking-[0.12em] block">{{ t('Citation Standard') }}</label>
                <Select v-model="editForm.citation">
                  <SelectTrigger class="h-11 border border-white/[0.08]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="IEEE / BibTeX Standard">IEEE / BibTeX Standard</SelectItem>
                    <SelectItem value="APA 7th Edition">APA 7th Edition</SelectItem>
                    <SelectItem value="Nature Standard">Nature Standard</SelectItem>
                    <SelectItem value="ACM Digital Library">ACM Digital Library</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div class="pt-2 flex items-center justify-end gap-3">
              <button
                type="button"
                @click="closeEditModal"
                class="px-5 py-2.5 rounded-xl bg-white/[0.06] hover:bg-white/10 text-white font-mono text-xs font-bold transition cursor-pointer"
              >
                {{ t('Cancel') }}
              </button>
              <button
                type="submit"
                class="px-6 py-2.5 rounded-xl bg-white hover:bg-cyan-100 text-blue-900 font-mono text-xs font-bold transition cursor-pointer"
              >
                {{ t('Save Profile ✓') }}
              </button>
            </div>
          </form>
        </div>
      </div>
    </Transition>
  </div>
</template>
