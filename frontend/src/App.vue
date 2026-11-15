<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthUserQuery } from '@/api/queries'
import AppTooltip from '@/components/ui/AppTooltip.vue'
import { TooltipProvider } from '@/components/ui/tooltip'
import { lang, toggleLang, t } from '@/composables/useLocale'
import ProfilePage from '@/pages/ProfilePage.vue'


const route = useRoute()
const router = useRouter()
const { data: currentUser } = useAuthUserQuery()

// Reactive Researcher Profile in Sidebar Dock
const profileName = ref('Guest Researcher')
const profileRole = ref('Not signed in')
const profileInitials = ref('?')
const profileAvatar = ref('')

function loadProfile() {
  const user = currentUser.value
  if (!user) {
    profileName.value = 'Guest Researcher'
    profileRole.value = 'Not signed in'
    profileInitials.value = '?'
    profileAvatar.value = ''
    return
  }

  let data: Record<string, string> = {}
  const saved = localStorage.getItem('sq_profile_data')
  if (saved) {
    try {
      data = JSON.parse(saved)
    } catch {
      data = {}
    }
  }

  const name = data.displayName || user.display_name || user.email?.split('@')[0] || 'Researcher'
  profileName.value = name
  profileRole.value = data.role || (user.role === 'admin' ? 'Administrator' : user.role) || 'Researcher'
  profileInitials.value = data.initials || name.slice(0, 2).toUpperCase() || 'SQ'
  profileAvatar.value = user.avatar_url || data.avatarUrl || ''
}

watch(currentUser, loadProfile, { immediate: true })

onMounted(() => {
  loadProfile()
  window.addEventListener('sq_profile_updated', loadProfile)
  window.addEventListener('keydown', onEscape)
  desktopMediaQuery = window.matchMedia('(min-width: 1024px)')
  desktopMediaQuery.addEventListener('change', syncProfileRoute)
})

onUnmounted(() => {
  window.removeEventListener('sq_profile_updated', loadProfile)
  window.removeEventListener('keydown', onEscape)
  desktopMediaQuery?.removeEventListener('change', syncProfileRoute)
})

// Expandable sidebar dock state (desktop)
const isExpanded = ref(true)
function toggleSidebar() {
  isExpanded.value = !isExpanded.value
}

const expandedView = computed(() => isExpanded.value)

const isProfilePopupOpen = ref(false)
let desktopMediaQuery: MediaQueryList | null = null

function openProfilePopup() {
  isProfilePopupOpen.value = true
}

function closeProfilePopup() {
  isProfilePopupOpen.value = false
}

function isDesktopViewport() {
  return typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches
}

function syncProfileRoute() {
  if (route.path === '/profile' && isDesktopViewport()) {
    isProfilePopupOpen.value = true
    router.replace('/dashboard')
    return
  }
  if (!isDesktopViewport() && isProfilePopupOpen.value) {
    isProfilePopupOpen.value = false
    router.push('/profile')
  }
}

function onEscape(event: KeyboardEvent) {
  if (event.key === 'Escape') closeProfilePopup()
}

watch(() => route.path, (path) => {
  syncProfileRoute()
  if (path !== '/dashboard' && path !== '/profile' && isProfilePopupOpen.value) {
    closeProfilePopup()
  }
}, { immediate: true })

const navItems = computed(() => {
  const items = [
    {
      name: 'Home',
      path: '/atlas',
      icon: 'atlas',
    },
    {
      name: 'Analytics',
      path: '/dashboard',
      icon: 'analytics',
    },
    {
      name: 'Corpus Database',
      path: '/database',
      icon: 'database',
    },
    {
      name: 'Research Input',
      path: '/submit-data',
      icon: 'submit',
    },
  ]
  if (currentUser.value?.role === 'admin') {
    items.push({
      name: 'Admin Panel',
      path: '/admin',
      icon: 'admin',
    })
  }
  return items
})

const mobileTabs = [
  { name: 'Home', shortName: 'Home', path: '/atlas', icon: 'atlas' },
  { name: 'Analytics', shortName: 'Analytics', path: '/dashboard', icon: 'analytics' },
  { name: 'Source Library', shortName: 'Sources', path: '/database', icon: 'database' },
  { name: 'About', shortName: 'About', path: '/about', icon: 'about' },
  { name: 'Profile', shortName: 'Profile', path: '/profile', icon: 'profile' },
]

const dockOffsetStyle = computed(() => ({
  '--dock-offset': isExpanded.value ? '322px' : '100px',
} as Record<string, string>))
</script>

<template>
  <TooltipProvider>
    <div class="h-screen w-screen bg-[#080f1b] text-zinc-100 flex flex-col lg:flex-row overflow-hidden font-sans antialiased">
      <!-- ── EXPANDABLE SIDEBAR / DOCK (w-[108px] collapsed, w-[330px] expanded) ────────────────── -->
      <div
        class="hidden lg:flex h-full flex-col flex-shrink-0 relative transition-all duration-300 ease-[cubic-bezier(0.2,0,0,1)]"
        :class="isExpanded ? 'w-[310px]' : 'w-[88px]'"
      >
      <aside
        class="flex-1 min-h-0 w-full bg-[#080f1b] flex flex-col justify-between py-5 shadow-2xl z-50 select-none border-r border-white/[0.08] relative overflow-hidden"
        :class="expandedView ? 'px-4' : 'px-3'"
      >
        <!-- Top: Brand Logo / Toggle Button (Smooth Accordion Transition) -->
        <div class="flex flex-col w-full">
          <!-- Smooth Sliding Brand Logo (Animates max-height & opacity instead of DOM jump) -->
          <div
            class="flex flex-col items-center overflow-hidden transition-all duration-300 ease-[cubic-bezier(0.2,0,0,1)]"
            :class="expandedView ? 'max-h-52 opacity-100 mb-3' : 'max-h-0 opacity-0 mb-0 pointer-events-none'"
          >
            <router-link
              to="/atlas"
              class="sidebar-brand flex flex-col items-center justify-center no-underline group pt-1"
              aria-label="SiamQuantum Atlas"
            >
              <div style="width:8.5rem;height:8.5rem;flex-shrink:0;display:flex;align-items:center;justify-content:center">
                <img class="sidebar-logo object-contain group-hover:scale-105 transition duration-300" src="/SQAlogo.png" alt="SiamQuantum Atlas Logo" width="136" height="136" decoding="async">
              </div>
            </router-link>
          </div>

          <!-- Navigation Icon Stack (Fixed icon container, zero layout jump) -->
          <nav class="flex flex-col gap-2 w-full" aria-label="Main navigation">
            <AppTooltip
              v-for="item in navItems"
              :key="item.path"
              :content="t(item.name)"
              side="right"
              :disabled="route.path === item.path || expandedView"
            >
            <router-link
              :to="item.path"
              class="h-[58px] rounded-[20px] flex items-center transition-colors duration-200 group relative w-full"
              :class="[
                expandedView ? 'px-2.5 justify-start' : 'px-0 justify-center',
                route.path === item.path
                  ? 'bg-white text-slate-950 font-bold shadow-lg shadow-white/10'
                  : 'text-slate-400 hover:text-white hover:bg-white/[0.06]'
              ]"
            >
              <!-- Fixed Icon Box (58px) -->
              <div class="w-[58px] min-w-[58px] h-[58px] flex-shrink-0 flex items-center justify-center transform-gpu">
                <!-- Dashboard Icon -->
                <svg v-if="item.icon === 'dashboard'" class="w-7 h-7 flex-shrink-0" viewBox="0 0 24 24" fill="currentColor">
                  <rect x="3" y="3" width="7" height="7" rx="2" />
                  <rect x="14" y="3" width="7" height="7" rx="2" />
                  <rect x="3" y="14" width="7" height="7" rx="2" />
                  <rect x="14" y="14" width="7" height="7" rx="2" />
                </svg>

                <!-- Atlas Globe Icon -->
                <svg v-else-if="item.icon === 'atlas'" class="w-7 h-7 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                  <circle cx="12" cy="12" r="9"/>
                  <path d="M3.5 9h17M3.5 15h17M12 3c2.4 2.4 3.7 5.3 3.7 9S14.4 18.6 12 21M12 3C9.6 5.4 8.3 8.3 8.3 12s1.3 6.6 3.7 9"/>
                </svg>

                <!-- Database Icon -->
                <svg v-else-if="item.icon === 'database'" class="w-7 h-7 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <ellipse cx="12" cy="6.5" rx="7" ry="3"/><path d="M5 6.5v5c0 1.66 3.13 3 7 3s7-1.34 7-3v-5"/><path d="M5 11.5v5c0 1.66 3.13 3 7 3s7-1.34 7-3v-5"/>
                </svg>

                <!-- Analytics Icon -->
                <svg v-else-if="item.icon === 'analytics'" class="w-7 h-7 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M4 18l5-6 4 3 7-9" />
                  <path d="M4 4v14h16" />
                </svg>


                <!-- About Icon -->
                <svg v-else-if="item.icon === 'about'" class="w-7 h-7 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <circle cx="12" cy="12" r="9" />
                  <path d="M12 11v5" />
                  <circle cx="12" cy="7.5" r="0.8" fill="currentColor" stroke="none" />
                </svg>

                <!-- Research Input Icon -->
                <svg v-else-if="item.icon === 'submit'" class="w-7 h-7 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                  <polyline points="17 8 12 3 7 8" />
                  <line x1="12" y1="3" x2="12" y2="15" />
                </svg>

                <!-- Admin Icon -->
                <svg v-else-if="item.icon === 'admin'" class="w-7 h-7 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z" />
                  <path d="M9 12l2 2 4-4" />
                </svg>
              </div>

              <!-- Smooth Sliding Label -->
              <span
                class="text-sm font-medium whitespace-nowrap overflow-hidden transition-all duration-300 ease-[cubic-bezier(0.2,0,0,1)] select-none"
                :class="expandedView ? 'opacity-100 max-w-[200px] pr-2' : 'opacity-0 max-w-0 pointer-events-none'"
              >
                {{ t(item.name) }}
              </span>
            </router-link>
            </AppTooltip>
          </nav>
        </div>

        <!-- Bottom: Analyst Avatar -->
        <div class="pt-3.5 border-t border-white/[0.06] w-full">
          <!-- About Link -->
          <AppTooltip :content="t('About')" side="top" :disabled="route.path === '/about' || expandedView">
            <router-link
              to="/about"
              class="mb-3 h-11 rounded-[16px] flex items-center transition-colors duration-200 w-full overflow-hidden"
              :class="[
                expandedView ? 'gap-3 px-2.5 justify-start' : 'justify-center',
                route.path === '/about'
                  ? 'bg-white text-slate-950'
                  : 'text-slate-400 hover:text-white hover:bg-white/[0.06]'
              ]"
            >
              <div class="w-9 h-9 flex-shrink-0 flex items-center justify-center">
                <svg class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <circle cx="12" cy="12" r="9" />
                  <path d="M12 11v5" />
                  <circle cx="12" cy="7.5" r="0.8" fill="currentColor" stroke="none" />
                </svg>
              </div>
              <span
                class="text-sm font-medium whitespace-nowrap overflow-hidden transition-all duration-300 ease-[cubic-bezier(0.2,0,0,1)] select-none"
                :class="expandedView ? 'opacity-100 max-w-[200px] pr-2' : 'opacity-0 max-w-0 pointer-events-none'"
              >
                {{ t('About') }}
              </span>
            </router-link>
          </AppTooltip>

          <!-- Language Toggle Row -->
          <div class="w-full mb-3">
            <AppTooltip :content="t('Switch language')" side="top">
              <button
                type="button"
                class="flex h-10 w-full items-center gap-2 rounded-[14px] bg-white/[0.04] px-2.5 text-slate-400 transition hover:bg-white/[0.07] hover:text-white"
                :class="expandedView ? 'justify-between' : 'justify-center'"
                :aria-label="t('Switch language')"
                @click="toggleLang"
              >
                <span v-if="expandedView" class="flex min-w-0 items-center gap-2">
                  <svg class="h-4 w-4 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="9" />
                    <path d="M3 12h18M12 3a15 15 0 0 1 0 18M12 3a15 15 0 0 0 0 18" />
                  </svg>
                  <span class="truncate text-xs font-medium">{{ lang === 'th' ? 'Thailand' : 'English' }}</span>
                </span>
                <span class="inline-flex items-center justify-center text-white/75">
                  <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M7 7h10l-3-3M17 17H7l3 3" />
                  </svg>
                </span>
              </button>
            </AppTooltip>
          </div>

          <!-- User Profile Avatar Link -->
          <AppTooltip :content="currentUser ? t('User Profile & Research Workspace') : t('Sign in to access your profile')" side="top" :disabled="expandedView">
          <button
            type="button"
            @click="openProfilePopup"
            class="rounded-[20px] px-2.5 py-2 flex items-center transition-all duration-300 overflow-hidden w-full group cursor-pointer bg-transparent border-0"
            :class="[
              isProfilePopupOpen ? 'bg-white/15' : 'bg-white/[0.04] hover:bg-white/[0.08]',
              expandedView ? 'gap-4 px-2.5' : 'justify-center'
            ]"
          >
            <div
              class="w-11 h-11 rounded-full bg-[#080f1b] flex items-center justify-center text-white text-sm font-bold font-mono flex-shrink-0 group-hover:scale-105 transition overflow-hidden"
              :class="isProfilePopupOpen ? 'ring-2 ring-cyan-300 text-cyan-300' : 'ring-2 ring-indigo-500/80'"
            >
              <img v-if="profileAvatar" :src="profileAvatar" alt="" class="h-full w-full object-cover">
              <span v-else>{{ profileInitials }}</span>
            </div>
            <div
              class="min-w-0 flex flex-col items-start text-left whitespace-nowrap overflow-hidden transition-all duration-300 ease-[cubic-bezier(0.2,0,0,1)]"
              :class="expandedView ? 'opacity-100 max-w-[170px]' : 'opacity-0 max-w-0 pointer-events-none'"
            >
              <span class="text-sm font-bold text-white group-hover:text-cyan-300 transition truncate">{{ profileName }}</span>
              <span class="text-xs font-mono text-slate-400 truncate">{{ profileRole }}</span>
            </div>
          </button>
          </AppTooltip>
        </div>
      </aside>

      <!-- Sidebar Toggle Tab (top-right edge, fully outside the sidebar) -->
      <AppTooltip
        :content="isExpanded ? t('Collapse Sidebar') : t('Expand Sidebar')"
        side="right"
      >
        <button
          @click="toggleSidebar"
          :aria-label="isExpanded ? t('Collapse Sidebar') : t('Expand Sidebar')"
          class="hidden lg:flex absolute top-4 left-full z-[60] w-8 h-16 rounded-r-2xl border border-white/[0.08] bg-[#080f1b] text-zinc-400 hover:text-white hover:bg-white/[0.10] items-center justify-center transition cursor-pointer shadow-lg"
        >
          <svg
            class="w-4 h-4 transition-transform duration-300 ease-out"
            :class="isExpanded ? 'rotate-180' : ''"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2.5"
          >
            <polyline points="9 18 15 12 9 6" />
          </svg>
        </button>
      </AppTooltip>
      </div>

      <!-- ── MAIN VIEWPORT (Zero-Scroll on Dashboard Desktop, Natural Scroll on Mobile/Tablet) ───── -->
      <div class="flex-1 min-h-0 min-w-0 flex flex-col overflow-hidden">
        <main
          class="flex-1 min-h-0 min-w-0 mx-0 overflow-hidden flex flex-col [scrollbar-gutter:stable_both-edges]"

          :class="[
            route.path === '/atlas' || route.path === '/map' || route.path === '/network'
              ? 'p-0 overflow-hidden'
              : route.path === '/dashboard'
                ? 'overflow-y-auto p-5 md:p-7 lg:pl-12'
                : route.path === '/profile'
                  ? 'overflow-y-auto p-5 md:p-7 lg:overflow-hidden lg:pl-12'
                  : 'overflow-y-auto p-5 md:p-7 lg:pl-12'
          ]"
        >
          <router-view />
        </main>
      </div>

      <!-- ── MOBILE TABS ───────────────────────────────────────────────── -->
      <nav
        class="lg:hidden flex-none z-50 border-t border-white/[0.08] bg-[#080f1b]/95 backdrop-blur-xl"
        aria-label="Mobile navigation"
      >
        <div class="flex w-full items-stretch justify-around px-1.5 pb-[env(safe-area-inset-bottom)]">
          <router-link
            v-for="tab in mobileTabs"
            :key="tab.path"
            :to="tab.path"
            class="group relative flex flex-1 flex-col items-center justify-center gap-1 min-h-[62px] px-0.5 py-2 text-[8px] font-semibold uppercase tracking-[0.03em] transition-colors no-underline"
            :class="route.path === tab.path ? 'text-white' : 'text-zinc-500 hover:text-zinc-200'"
          >
            <span
              class="flex h-9 w-12 items-center justify-center rounded-2xl transition-all duration-200"
              :class="route.path === tab.path
                ? 'bg-white text-slate-950 shadow-lg shadow-white/10'
                : 'group-hover:bg-white/[0.06]'"
            >
              <svg v-if="tab.icon === 'dashboard'" class="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                <rect x="3" y="3" width="7" height="7" rx="2" />
                <rect x="14" y="3" width="7" height="7" rx="2" />
                <rect x="3" y="14" width="7" height="7" rx="2" />
                <rect x="14" y="14" width="7" height="7" rx="2" />
              </svg>
              <svg v-else-if="tab.icon === 'atlas'" class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="9" />
                <path d="M3.5 9h17M3.5 15h17M12 3c2.4 2.4 3.7 5.3 3.7 9S14.4 18.6 12 21M12 3C9.6 5.4 8.3 8.3 8.3 12s1.3 6.6 3.7 9" />
              </svg>
              <svg v-else-if="tab.icon === 'analytics'" class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M4 18l5-6 4 3 7-9" />
                <path d="M4 4v14h16" />
              </svg>
              <svg v-else-if="tab.icon === 'database'" class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <ellipse cx="12" cy="6.5" rx="7" ry="3" />
                <path d="M5 6.5v5c0 1.66 3.13 3 7 3s7-1.34 7-3v-5" />
                <path d="M5 11.5v5c0 1.66 3.13 3 7 3s7-1.34 7-3v-5" />
              </svg>
              <svg v-else-if="tab.icon === 'about'" class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="9" />
                <path d="M12 11v5" />
                <circle cx="12" cy="7.5" r="0.8" fill="currentColor" stroke="none" />
              </svg>
              <svg v-else-if="tab.icon === 'profile'" class="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="8" r="4" />
                <path d="M4 21a8 8 0 0 1 16 0" />
              </svg>
            </span>
          </router-link>
        </div>
      </nav>

      <!-- ── DESKTOP PROFILE POPUP ─────────────────────────────────────── -->
      <Teleport to="body">
        <Transition
          enter-active-class="transition duration-300 ease-out"
          enter-from-class="opacity-0 [&>div]:translate-y-24 [&>div]:scale-[0.98]"
          enter-to-class="opacity-100 [&>div]:translate-y-0 [&>div]:scale-100"
          leave-active-class="transition duration-200 ease-in"
          leave-from-class="opacity-100 [&>div]:translate-y-0 [&>div]:scale-100"
          leave-to-class="opacity-0 [&>div]:translate-y-24 [&>div]:scale-[0.98]"
        >
          <div
            v-if="isProfilePopupOpen"
            class="fixed inset-0 z-[1500] bg-black/80 backdrop-blur-md flex items-center justify-center p-3 sm:p-6 lg:p-8"
            role="dialog"
            aria-modal="true"
            aria-label="Profile"
            @click.self="closeProfilePopup"
          >
            <div class="relative w-full max-w-[1200px]">
              <button
                type="button"
                @click="closeProfilePopup"
                class="absolute -right-2 -top-2 z-20 flex h-9 w-9 items-center justify-center rounded-full border border-white/15 bg-[#080f1b] text-white/70 shadow-lg transition hover:bg-white/10 hover:text-white sm:-right-3 sm:-top-3"
                aria-label="Close profile"
              >
                <svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round">
                  <path d="M18 6 6 18M6 6l12 12" />
                </svg>
              </button>
              <div class="max-h-[calc(100vh-2rem)] w-full overflow-hidden rounded-[28px] border border-white/[0.12] bg-[#080f1b] shadow-2xl transition-all duration-300 ease-out">
                <div class="max-h-[calc(100vh-2rem)] overflow-hidden p-4 sm:p-6 lg:p-7">
                  <ProfilePage />
                </div>
              </div>
            </div>
          </div>
        </Transition>
      </Teleport>
    </div>
  </TooltipProvider>
</template>
