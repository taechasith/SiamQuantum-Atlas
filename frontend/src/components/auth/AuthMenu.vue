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

withDefaults(defineProps<{ variant?: 'default' | 'profile' | 'cta' }>(), {
  variant: 'default',
})

const queryClient = useQueryClient()
const { data: user, refetch } = useAuthUserQuery()
const loginMutation = useLocalLoginMutation()
const registerMutation = useLocalRegisterMutation()
const logoutMutation = useLocalLogoutMutation()

const storedEmail =
  typeof localStorage !== 'undefined' ? localStorage.getItem('sq_remember_email') ?? '' : ''

const showLoginModal = ref(false)
const mode = ref<'login' | 'register'>('login')
const loginEmail = ref(storedEmail)
const rememberEmail = ref(Boolean(storedEmail))
const loginPassword = ref('')
const loginError = ref('')
const isLoggingIn = ref(false)
const showPassword = ref(false)

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
    if (rememberEmail.value) localStorage.setItem('sq_remember_email', loginEmail.value)
    else localStorage.removeItem('sq_remember_email')
    await refetch()
    showLoginModal.value = false
  } catch (err: any) {
    loginError.value = err.message || t('Login failed')
  } finally {
    isLoggingIn.value = false
  }
}

function openAuth() {
  mode.value = 'login'
  loginError.value = ''
  showPassword.value = false
  showLoginModal.value = true
}

function switchMode() {
  mode.value = mode.value === 'login' ? 'register' : 'login'
  loginError.value = ''
  loginEmail.value = ''
  loginPassword.value = ''
  showPassword.value = false
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
    <template v-else>
      <button
        v-if="variant === 'profile'"
        type="button"
        class="min-h-[42px] rounded-xl bg-[#ff4c2b] px-5 py-2.5 text-sm font-bold text-white transition hover:bg-[#e84324]"
        @click="openAuth"
      >
        {{ t('Login') }}
      </button>
      <button
        v-else-if="variant === 'cta'"
        type="button"
        class="px-5 py-2.5 rounded-2xl bg-[#ff4c2b] hover:bg-[#e84324] text-white font-mono text-xs font-bold transition whitespace-nowrap"
        @click="openAuth"
      >
        {{ t('Sign In') }}
      </button>
      <button
        v-else
        type="button"
        class="min-h-[38px] rounded-full bg-[#ff4c2b] px-5 py-2.5 text-sm font-bold text-white transition hover:bg-[#e84324]"
        @click="openAuth"
      >
        {{ t('Sign In') }}
      </button>
    </template>

    <Teleport to="body">
      <div
        v-if="showLoginModal"
        class="fixed inset-0 z-[3000] flex items-center justify-center bg-[#080f1b]/85 backdrop-blur-xl"
        @click.self="showLoginModal = false"
      >
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="login-modal-title"
          class="relative grid h-[100dvh] max-h-none w-full max-w-none overflow-hidden bg-white text-slate-900 lg:grid-cols-[1.02fr_0.98fr]"
        >
          <button
            type="button"
            :aria-label="t('Close')"
            class="absolute right-4 top-4 z-30 flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition hover:bg-slate-200 hover:text-slate-900"
            @click="showLoginModal = false"
          >
            <svg class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round">
              <path d="M6 6l12 12M18 6 6 18" />
            </svg>
          </button>

          <div class="relative hidden h-full min-h-0 flex-col justify-between overflow-hidden bg-[#080f1b] p-8 text-white lg:flex xl:p-10">
            <div class="pointer-events-none absolute inset-0">
              <div class="absolute -left-40 top-10 h-[420px] w-[900px] -rotate-12 rounded-[50%] border border-white/[0.06]" />
              <div class="absolute -left-28 top-24 h-[380px] w-[780px] -rotate-12 rounded-[50%] border border-white/[0.04]" />
              <div class="absolute -right-32 bottom-0 h-[420px] w-[760px] rotate-12 rounded-[50%] border border-white/[0.05]" />
              <div class="absolute inset-0 bg-[radial-gradient(circle_at_18%_12%,rgba(50,157,210,.22),transparent_42%)]" />
              <div class="absolute inset-0 bg-[radial-gradient(circle_at_82%_84%,rgba(255,76,43,.13),transparent_38%)]" />
            </div>

            <div class="relative z-10">
              <img
                src="/SQAlogo.png"
                alt="SiamQuantum Atlas"
                class="h-12 w-auto object-contain"
                width="1614"
                height="1030"
                decoding="async"
              />
            </div>

            <div class="relative z-10 mt-auto">
              <h3 class="max-w-[560px] text-5xl font-black leading-[1.04] tracking-tight xl:text-6xl">
                <span class="block">{{ t('See every quantum signal.') }}</span>
                <span class="block">{{ t('Decide with confidence.') }}</span>
              </h3>
            </div>
          </div>

          <div class="flex h-full flex-col justify-center overflow-y-auto p-6 sm:p-9 lg:p-10 xl:p-12">
            <div class="mx-auto w-full max-w-[360px]">
              <img
                src="/SQAlogo.png"
                alt="SiamQuantum Atlas"
                class="mb-8 h-10 w-auto object-contain brightness-0 lg:hidden"
                width="1614"
                height="1030"
                decoding="async"
              />

              <h2 id="login-modal-title" class="m-0 text-3xl font-black tracking-tight text-[#080f1b] sm:text-4xl">
                {{ mode === 'register' ? t('Create your SiamQuantum account') : t('Welcome back') }}
              </h2>
              <p class="mt-2 text-sm leading-6 text-slate-500">
                {{ mode === 'register' ? t('Start managing your corpus and research workspace.') : t('Access your corpus and research workspace.') }}
              </p>

              <div v-if="loginError" class="mt-5 rounded-xl bg-rose-50 px-4 py-3 text-xs leading-5 text-rose-600">
                {{ loginError }}
              </div>

              <form @submit.prevent="handleLogin" class="mt-8 space-y-5">
                <div>
                  <label for="login-email" class="mb-2 block text-xs font-bold text-slate-700">{{ t('Email') }}</label>
                  <input
                    id="login-email"
                    v-model="loginEmail"
                    type="email"
                    name="email"
                    autocomplete="email"
                    required
                    class="h-12 w-full rounded-xl bg-slate-100 px-4 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:ring-2 focus:ring-[#ff4c2b]/30"
                    placeholder="name@organization.com"
                  />
                </div>
                <div>
                  <label for="login-password" class="mb-2 block text-xs font-bold text-slate-700">{{ t('Password') }}</label>
                  <div class="relative">
                    <input
                      id="login-password"
                      v-model="loginPassword"
                      :type="showPassword ? 'text' : 'password'"
                      name="password"
                      :autocomplete="mode === 'register' ? 'new-password' : 'current-password'"
                      required
                      class="h-12 w-full rounded-xl bg-slate-100 px-4 pr-11 text-sm text-slate-900 outline-none transition placeholder:text-slate-400 focus:ring-2 focus:ring-[#ff4c2b]/30"
                      :placeholder="t('At least 8 characters')"
                    />
                    <button
                      type="button"
                      class="absolute right-3 top-1/2 flex h-7 w-7 -translate-y-1/2 items-center justify-center rounded-full text-slate-400 transition hover:bg-slate-200 hover:text-slate-700"
                      :aria-label="showPassword ? t('Hide password') : t('Show password')"
                      @click="showPassword = !showPassword"
                    >
                      <svg v-if="!showPassword" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M2 12s3.5-6 10-6 10 6 10 6-3.5 6-10 6S2 12 2 12Z" />
                        <circle cx="12" cy="12" r="2.5" />
                      </svg>
                      <svg v-else class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="m3 3 18 18" />
                        <path d="M10.6 6.2A10.8 10.8 0 0 1 12 6c6.5 0 10 6 10 6a17 17 0 0 1-3.1 3.8" />
                        <path d="M6.2 6.7A17.4 17.4 0 0 0 2 12s3.5 6 10 6a10.7 10.7 0 0 0 4.4-.9" />
                      </svg>
                    </button>
                  </div>
                </div>

                <div class="flex items-center gap-3">
                  <label class="flex cursor-pointer items-center gap-2 text-xs text-slate-500">
                    <input
                      v-model="rememberEmail"
                      type="checkbox"
                      class="h-4 w-4 rounded border-slate-300 accent-[#ff4c2b] [color-scheme:light]"
                    />
                    {{ t('Remember email') }}
                  </label>
                </div>

                <button
                  type="submit"
                  :disabled="isLoggingIn"
                  class="flex h-12 w-full items-center justify-center gap-2 rounded-full bg-[#ff4c2b] px-5 text-sm font-bold text-white transition hover:bg-[#e84324] disabled:cursor-not-allowed disabled:opacity-50 active:scale-[.99]"
                >
                  <span v-if="isLoggingIn" class="h-4 w-4 animate-spin rounded-full border-2 border-white border-t-transparent" />
                  <span>{{ isLoggingIn ? t('Signing in...') : mode === 'register' ? t('Create account') : t('Sign In') }}</span>
                  <svg v-if="!isLoggingIn" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M5 12h14" />
                    <path d="m13 6 6 6-6 6" />
                  </svg>
                </button>

                <div class="pt-2">
                  <button
                    type="button"
                    class="flex h-12 w-full items-center justify-center rounded-full bg-transparent text-sm font-bold text-[#ff4c2b] transition hover:bg-transparent hover:text-[#e84324] active:bg-transparent"
                    @click="switchMode"
                  >
                    {{ mode === 'register' ? t('Sign In') : t('Sign up') }}
                  </button>
                </div>
              </form>

              <p class="mt-6 text-center text-[10px] text-slate-400">© 2026 SiamQuantum · {{ t('All rights reserved') }}</p>
            </div>
          </div>
        </div>
      </div>
    </Teleport>
  </div>
</template>
