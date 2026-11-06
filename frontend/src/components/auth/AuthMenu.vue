<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQueryClient } from '@tanstack/vue-query'
import {
  useAuthUserQuery,
  useLocalLoginMutation,
  useLocalLogoutMutation,
  useLocalRegisterMutation,
} from '@/api/queries'
import { t } from '@/composables/useLocale'

const queryClient = useQueryClient()
const { data: user, refetch } = useAuthUserQuery()
const loginMutation = useLocalLoginMutation()
const registerMutation = useLocalRegisterMutation()
const logoutMutation = useLocalLogoutMutation()

const showLoginModal = ref(false)
const mode = ref<'login' | 'register'>('login')
const loginEmail = ref('demo@siamquantum.org')
const loginPassword = ref('password123')
const loginError = ref('')
const isLoggingIn = ref(false)

const displayName = computed(
  () => user.value?.display_name || user.value?.email?.split('@')[0] || 'Researcher'
)

async function handleLogin() {
  if (!loginEmail.value) return
  isLoggingIn.value = true
  loginError.value = ''
  try {
    if (mode.value === 'register') {
      await registerMutation.mutateAsync({
        email: loginEmail.value,
        password: loginPassword.value,
      })
    } else {
      await loginMutation.mutateAsync({
        email: loginEmail.value,
        password: loginPassword.value,
      })
    }
    await refetch()
    showLoginModal.value = false
  } catch (err: any) {
    loginError.value = err.message || t('Login failed')
  } finally {
    isLoggingIn.value = false
  }
}

function switchMode() {
  mode.value = mode.value === 'login' ? 'register' : 'login'
  loginError.value = ''
  if (mode.value === 'register') {
    loginEmail.value = ''
    loginPassword.value = ''
  } else {
    loginEmail.value = 'demo@siamquantum.org'
    loginPassword.value = 'password123'
  }
}

async function handleLogout() {
  await logoutMutation.mutateAsync()
  queryClient.setQueryData(['auth', 'me'], null)
  await refetch()
}
</script>

<template>
  <div class="flex items-center gap-2">
    <template v-if="user">
      <span class="hidden max-w-[160px] truncate text-[10px] font-mono text-white/55 sm:inline">
        {{ t('Welcome ') }}<span class="font-bold text-white">{{ displayName }}</span>
      </span>
      <button
        type="button"
        class="flex h-8 w-8 items-center justify-center rounded-full border border-white/10 bg-white/[0.06] text-white/55 transition hover:border-rose-300/30 hover:bg-rose-500/15 hover:text-rose-200"
        :aria-label="t('Sign Out')"
        :title="t('Sign Out')"
        @click="handleLogout"
      >
        <svg class="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
          <polyline points="16 17 21 12 16 7" />
          <line x1="21" y1="12" x2="9" y2="12" />
        </svg>
      </button>
    </template>
    <button
      v-else
      type="button"
      class="rounded-full bg-white px-3.5 py-1.5 text-[10px] font-bold text-slate-950 transition hover:bg-sky-100"
      @click="showLoginModal = true"
    >
      {{ t('Sign In') }}
    </button>

    <Teleport to="body">
      <div
        v-if="showLoginModal"
        class="fixed inset-0 z-[100] flex items-center justify-center bg-[#020617]/80 p-4 backdrop-blur-xl"
        @click.self="showLoginModal = false"
      >
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="login-modal-title"
          class="relative max-h-[calc(100dvh-2rem)] w-full max-w-[440px] overflow-y-auto rounded-[30px] border border-sky-200/15 bg-[#040914] p-6 text-white shadow-[0_30px_120px_rgba(2,6,23,.85)] animate-fade-in sm:p-7"
        >
          <div class="pointer-events-none absolute inset-x-0 top-0 h-44 bg-[linear-gradient(180deg,rgba(29,78,216,.42)_0%,rgba(29,78,216,.12)_55%,transparent_100%)]" />

          <button
            type="button"
            :aria-label="t('Close')"
            class="absolute right-4 top-4 z-10 flex h-9 w-9 items-center justify-center rounded-full border border-white/10 bg-white/[0.04] text-white/45 transition hover:border-white/20 hover:bg-white/10 hover:text-white"
            @click="showLoginModal = false"
          >
            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
              <path d="M6 6l12 12M18 6 6 18" />
            </svg>
          </button>

          <div class="relative text-center">
            <div class="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-2xl border border-sky-200/20 bg-[linear-gradient(135deg,#1d4ed8_0%,#0b2a68_100%)] text-white shadow-[0_12px_30px_rgba(29,78,216,.35)]">
              <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4" />
                <polyline points="10 17 15 12 10 7" />
                <line x1="15" y1="12" x2="3" y2="12" />
              </svg>
            </div>
            <p class="font-mono text-[9px] uppercase tracking-[0.24em] text-sky-100/45">SiamQuantum Atlas</p>
            <h2 id="login-modal-title" class="mt-1.5 text-xl font-black tracking-tight text-white">
              {{ mode === 'register' ? t('Create your SiamQuantum account') : t('Sign in to SiamQuantum') }}
            </h2>
            <p class="mx-auto mt-1.5 max-w-[300px] text-xs leading-5 text-white/50">{{ t('Authenticate to access analytics and reporting privileges.') }}</p>
          </div>

          <div v-if="loginError" class="relative mb-4 mt-5 rounded-xl border border-rose-300/20 bg-rose-500/10 px-3.5 py-3 text-xs leading-5 text-rose-200">
            {{ loginError }}
          </div>

          <form @submit.prevent="handleLogin" class="relative mt-6 space-y-4">
            <div>
              <label for="login-email" class="mb-1.5 block font-mono text-[10px] uppercase tracking-[0.16em] text-sky-100/55">{{ t('Email') }}</label>
              <input
                id="login-email"
                v-model="loginEmail"
                type="email"
                name="email"
                autocomplete="email"
                required
                class="w-full rounded-xl border border-white/10 bg-white/[0.045] px-3.5 py-3 font-mono text-sm text-white outline-none transition placeholder:text-white/25 focus:border-sky-300/60 focus:bg-white/[0.07] focus:ring-2 focus:ring-sky-400/10"
                placeholder="name@organization.com"
              />
            </div>
            <div>
              <label for="login-password" class="mb-1.5 block font-mono text-[10px] uppercase tracking-[0.16em] text-sky-100/55">{{ t('Password') }}</label>
              <input
                id="login-password"
                v-model="loginPassword"
                type="password"
                name="password"
                autocomplete="current-password"
                required
                class="w-full rounded-xl border border-white/10 bg-white/[0.045] px-3.5 py-3 font-mono text-sm text-white outline-none transition placeholder:text-white/25 focus:border-sky-300/60 focus:bg-white/[0.07] focus:ring-2 focus:ring-sky-400/10"
                placeholder="••••••••"
              />
            </div>

            <button
              type="submit"
              :disabled="isLoggingIn"
              class="flex w-full items-center justify-center gap-2 rounded-xl bg-white px-4 py-3 text-sm font-bold text-slate-950 transition hover:bg-sky-100 disabled:cursor-not-allowed disabled:opacity-50 active:scale-[.99]"
            >
              <span v-if="isLoggingIn" class="h-4 w-4 animate-spin rounded-full border-2 border-slate-950 border-t-transparent" />
              <span>{{ isLoggingIn ? t('Signing in...') : mode === 'register' ? t('Create account') : t('Sign In') }}</span>
            </button>
          </form>

          <div class="relative mt-4 text-center text-[11px] text-white/45">
            <button type="button" class="font-bold text-sky-200 transition hover:text-white" @click="switchMode">
              {{ mode === 'register' ? t('Already have an account? Sign in') : t('New here? Create account') }}
            </button>
          </div>

          <div class="relative mt-4 flex flex-col gap-2 border-t border-white/10 pt-4 font-mono text-[10px] leading-5 text-white/35 sm:flex-row sm:items-center sm:justify-between">
            <span>{{ t('Demo account: demo@siamquantum.org') }}</span>
            <button
              type="button"
              class="text-left font-bold text-sky-200 transition hover:text-white sm:text-right"
              @click="loginEmail = 'demo@siamquantum.org'; loginPassword = 'password123'; handleLogin()"
            >
              {{ t('Click to log in instantly ↗') }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>
