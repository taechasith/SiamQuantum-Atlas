<script setup lang="ts">
import { computed, ref } from 'vue'
import {
  useAuthUserQuery,
  useAdminSubmissionsQuery,
  useAdminUpdateSubmissionMutation,
  useAdminUsersQuery,
  useAdminUpdateUserRoleMutation,
  useAdminAuditLogQuery,
  type AdminSubmission,
  type AdminUser,
} from '@/api/queries'
import AppTooltip from '@/components/ui/AppTooltip.vue'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { t } from '@/composables/useLocale'

const { data: authUser } = useAuthUserQuery()
const isAdmin = computed(() => authUser.value?.role === 'admin')

const activeTab = ref<'submissions' | 'access' | 'audit'>('submissions')
const status = ref('pending')
const category = ref('all')
const userSearch = ref('')
const actionError = ref('')

const { data: submissions, isPending, isError, error, refetch, isFetching } =
  useAdminSubmissionsQuery(status, category, isAdmin)
const { mutateAsync: patchAdminSubmission, isPending: isUpdating } = useAdminUpdateSubmissionMutation()

const { data: adminUsers, isPending: isUsersPending, refetch: refetchUsers } =
  useAdminUsersQuery(isAdmin, userSearch)
const { mutateAsync: patchUserRole, isPending: isRoleUpdating } = useAdminUpdateUserRoleMutation()

const { data: auditLog, isPending: isAuditPending } = useAdminAuditLogQuery(isAdmin)

const errorMessage = computed(() => (error.value as Error | null)?.message ?? '')
const adminCount = computed(() => (adminUsers.value ?? []).filter((user) => user.role === 'admin').length)

async function updateUserRole(user: AdminUser, role: 'admin' | 'user') {
  actionError.value = ''
  try {
    await patchUserRole({ id: user.id, role })
  } catch (err: any) {
    actionError.value = err?.message || t('Failed to update role')
  }
}

const statusOptions = [
  { value: 'all', label: 'All Statuses' },
  { value: 'pending', label: 'Pending' },
  { value: 'approved', label: 'Approved' },
  { value: 'rejected', label: 'Rejected' },
]

const categories = [
  'Quantum Computing',
  'Quantum Sensing',
  'Quantum Communications',
  'Quantum Information',
  'Quantum Hardware & Materials',
  'Quantum Education & Policy',
]

const pendingCount = computed(
  () => (submissions.value ?? []).filter((item) => item.status === 'pending').length
)

function statusLabel(item: AdminSubmission) {
  const primary = item.status || 'pending'
  const analysis = item.analysis_status ? ` / ${item.analysis_status}` : ''
  return `${primary}${analysis}`
}

function formatDate(value?: string) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })
}

async function patchSubmission(item: AdminSubmission, patch: Record<string, string>) {
  actionError.value = ''
  try {
    await patchAdminSubmission({ id: item.id, patch })
  } catch (err: any) {
    actionError.value = err?.message || t('Failed to update submission')
  }
}
</script>

<template>
  <div class="w-full select-none pb-6 space-y-5">
    <!-- Header -->
    <div class="flex flex-col sm:flex-row sm:items-end justify-between gap-3">
      <div class="flex items-center gap-2.5">
        <h1 class="text-2xl sm:text-[36px] font-black text-white tracking-tight leading-tight m-0 uppercase">
          {{ t('Admin Review Panel') }}
        </h1>
        <AppTooltip :content="t('Review submitted research before it is published to the Atlas.')" side="bottom">
          <button
            type="button"
            class="inline-flex h-8 w-8 items-center justify-center rounded-full text-white/50 hover:text-white hover:bg-white/10 transition cursor-pointer"
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
      <span class="self-start sm:self-auto px-3 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold uppercase tracking-[0.12em] text-white">
        {{ t('Admin') }}
      </span>
    </div>

    <!-- Access denied -->
    <div
      v-if="!isAdmin"
      class="rounded-[30px] p-6 sm:p-10 text-white flex flex-col sm:flex-row items-start sm:items-center justify-between gap-5"
      style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
    >
      <div class="flex items-center gap-4">
        <div class="w-14 h-14 rounded-2xl bg-black/40 flex items-center justify-center flex-shrink-0">
          <svg class="w-6 h-6 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z" />
            <path d="M9 12l2 2 4-4" />
          </svg>
        </div>
        <div>
          <h2 class="text-lg sm:text-xl font-bold m-0">{{ t('Admin access required') }}</h2>
          <p class="text-xs sm:text-sm text-sky-200/80 m-0 mt-1">{{ t('Only accounts with the admin role can review submitted research.') }}</p>
        </div>
      </div>
      <router-link
        to="/dashboard"
        class="px-5 py-2.5 rounded-2xl bg-white hover:bg-cyan-100 text-blue-900 font-mono text-xs font-bold no-underline transition whitespace-nowrap"
      >
        {{ t('Go to Dashboard') }}
      </router-link>
    </div>

    <template v-else>
      <!-- Tabs -->
      <div class="flex items-center gap-1 border-b border-white/20">
        <button
          type="button"
          @click="activeTab = 'submissions'"
          class="relative px-4 sm:px-5 py-2.5 text-[11px] sm:text-xs font-mono font-bold uppercase tracking-[0.14em] transition cursor-pointer"
          :class="activeTab === 'submissions' ? 'text-white' : 'text-white/50 hover:text-white/80'"
        >
          {{ t('Submissions') }}
          <span v-if="activeTab === 'submissions'" class="absolute -bottom-px left-2 right-2 h-0.5 rounded-full bg-cyan-300" />
        </button>
        <button
          type="button"
          @click="activeTab = 'access'"
          class="relative px-4 sm:px-5 py-2.5 text-[11px] sm:text-xs font-mono font-bold uppercase tracking-[0.14em] transition cursor-pointer"
          :class="activeTab === 'access' ? 'text-white' : 'text-white/50 hover:text-white/80'"
        >
          {{ t('Access Control') }}
          <span v-if="activeTab === 'access'" class="absolute -bottom-px left-2 right-2 h-0.5 rounded-full bg-cyan-300" />
        </button>
        <button
          type="button"
          @click="activeTab = 'audit'"
          class="relative px-4 sm:px-5 py-2.5 text-[11px] sm:text-xs font-mono font-bold uppercase tracking-[0.14em] transition cursor-pointer"
          :class="activeTab === 'audit' ? 'text-white' : 'text-white/50 hover:text-white/80'"
        >
          {{ t('Audit Log') }}
          <span v-if="activeTab === 'audit'" class="absolute -bottom-px left-2 right-2 h-0.5 rounded-full bg-cyan-300" />
        </button>
      </div>

      <p v-if="actionError" class="text-xs font-mono text-rose-300 bg-rose-500/10 rounded-xl px-4 py-3 m-0">
        {{ actionError }}
      </p>

      <div v-if="activeTab === 'submissions'" class="space-y-5">
      <!-- Filters -->
      <section
        class="rounded-[26px] p-4 sm:p-5 text-white flex flex-col lg:flex-row lg:items-center justify-between gap-4"
        style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
      >
        <div class="flex flex-wrap items-center gap-3">
          <Select v-model="status">
            <SelectTrigger class="w-[148px]">
              <SelectValue :placeholder="t('All Statuses')" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem v-for="option in statusOptions" :key="option.value" :value="option.value">
                {{ t(option.label) }}
              </SelectItem>
            </SelectContent>
          </Select>

          <Select v-model="category">
            <SelectTrigger class="w-[190px]">
              <SelectValue :placeholder="t('All Categories')" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">{{ t('All Categories') }}</SelectItem>
              <SelectItem v-for="item in categories" :key="item" :value="item">
                {{ t(item) }}
              </SelectItem>
            </SelectContent>
          </Select>

          <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white">
            {{ submissions?.length ?? 0 }} {{ t('submissions') }}
          </span>
          <span v-if="pendingCount" class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white">
            {{ pendingCount }} {{ t('pending') }}
          </span>
        </div>

        <button
          type="button"
          class="self-start lg:self-auto px-4 py-2.5 rounded-xl bg-white hover:bg-cyan-100 text-blue-900 font-mono text-xs font-bold transition cursor-pointer flex items-center gap-2"
          :disabled="isFetching"
          @click="refetch()"
        >
          <svg class="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
            <path d="M3 3v5h5" />
          </svg>
          {{ isFetching ? t('Refreshing...') : t('Refresh') }}
        </button>
      </section>

      <!-- Loading -->
      <div v-if="isPending" class="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <div
          v-for="i in 4"
          :key="i"
          class="rounded-[26px] h-44 animate-pulse bg-white/[0.04]"
        />
      </div>

      <div v-else-if="isError" class="rounded-[26px] p-6 text-white" style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);">
        <p class="text-sm font-bold m-0">{{ t('Unable to load submissions') }}</p>
        <p class="text-xs text-sky-200/80 m-0 mt-1">{{ errorMessage }}</p>
      </div>

      <!-- Empty -->
      <div
        v-else-if="!submissions?.length"
        class="rounded-[26px] p-10 text-white text-center"
        style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.35) 55%, rgba(29, 78, 216, 0) 100%);"
      >
        <p class="text-base font-bold m-0">{{ t('No submissions match the current filters') }}</p>
        <p class="text-xs text-sky-200/80 m-0 mt-1">{{ t('Try changing the status or category filter.') }}</p>
      </div>

      <!-- Submission list -->
      <div v-else class="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <article
          v-for="item in submissions"
          :key="item.id"
          class="rounded-[26px] p-5 text-white flex flex-col justify-between gap-4"
          style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
        >
          <div class="space-y-3">
            <div class="flex items-start justify-between gap-3">
              <div class="min-w-0">
                <h2 class="text-base sm:text-lg font-bold leading-snug m-0 break-words">
                  {{ item.title || item.source_url || '—' }}
                </h2>
                <p class="text-[11px] font-mono text-sky-200/80 m-0 mt-1">
                  #{{ item.id }} · {{ formatDate(item.created_at) }}
                  <span v-if="item.submitted_by"> · {{ item.submitted_by }}</span>
                </p>
              </div>
              <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[9px] font-mono font-bold uppercase text-white whitespace-nowrap">
                {{ statusLabel(item) }}
              </span>
            </div>

            <div class="flex flex-wrap items-center gap-2">
              <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white">
                {{ item.category ? t(item.category) : t('Uncategorized') }}
              </span>
              <a
                v-if="item.source_url"
                :href="item.source_url"
                target="_blank"
                rel="noopener noreferrer"
                class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white no-underline hover:bg-black/70 transition truncate max-w-[240px]"
              >
                {{ t('Open Source') }} ↗
              </a>
            </div>
          </div>

          <div class="flex flex-wrap items-center gap-2">
            <button
              v-if="item.status !== 'approved'"
              type="button"
              class="px-3.5 py-2 rounded-xl bg-white hover:bg-cyan-100 text-blue-900 font-mono text-[11px] font-bold transition cursor-pointer disabled:opacity-50"
              :disabled="isUpdating"
              @click="patchSubmission(item, { status: 'approved' })"
            >
              {{ t('Approve') }}
            </button>
            <button
              v-if="item.status !== 'rejected'"
              type="button"
              class="px-3.5 py-2 rounded-xl bg-black/35 hover:bg-black/50 text-white font-mono text-[11px] font-bold transition cursor-pointer disabled:opacity-50"
              :disabled="isUpdating"
              @click="patchSubmission(item, { status: 'rejected' })"
            >
              {{ t('Reject') }}
            </button>
            <button
              v-if="item.analysis_status !== 'completed'"
              type="button"
              class="px-3.5 py-2 rounded-xl bg-black/35 hover:bg-black/50 text-white font-mono text-[11px] font-bold transition cursor-pointer disabled:opacity-50"
              :disabled="isUpdating"
              @click="patchSubmission(item, { analysis_status: 'completed' })"
            >
              {{ t('Mark Completed') }}
            </button>
            <button
              v-else
              type="button"
              class="px-3.5 py-2 rounded-xl bg-black/35 hover:bg-black/50 text-white font-mono text-[11px] font-bold transition cursor-pointer disabled:opacity-50"
              :disabled="isUpdating"
              @click="patchSubmission(item, { analysis_status: 'processing' })"
            >
              {{ t('Set Processing') }}
            </button>
          </div>
        </article>
      </div>

      </div>

      <!-- Access control -->
      <div v-else-if="activeTab === 'access'" class="space-y-5">
        <section
          class="rounded-[26px] p-4 sm:p-5 text-white flex flex-col lg:flex-row lg:items-center justify-between gap-4"
          style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
        >
          <div class="flex flex-wrap items-center gap-3">
            <input
              v-model="userSearch"
              type="text"
              :placeholder="t('Search users by email or name...')"
              class="w-full sm:w-72 px-3.5 py-2.5 rounded-xl bg-black/35 text-xs font-mono text-white placeholder-white/40 focus:outline-none focus:ring-2 focus:ring-cyan-300/40 transition"
            >
            <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white">
              {{ adminUsers?.length ?? 0 }} {{ t('users') }}
            </span>
            <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[10px] font-mono font-bold text-white">
              {{ adminCount }} {{ t('admins') }}
            </span>
          </div>
          <button
            type="button"
            class="self-start lg:self-auto px-4 py-2.5 rounded-xl bg-white hover:bg-cyan-100 text-blue-900 font-mono text-xs font-bold transition cursor-pointer"
            @click="refetchUsers()"
          >
            {{ t('Refresh') }}
          </button>
        </section>

        <div v-if="isUsersPending" class="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <div v-for="i in 4" :key="i" class="rounded-[26px] h-28 animate-pulse bg-white/[0.04]" />
        </div>

        <div
          v-else-if="!adminUsers?.length"
          class="rounded-[26px] p-10 text-white text-center"
          style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.35) 55%, rgba(29, 78, 216, 0) 100%);"
        >
          <p class="text-base font-bold m-0">{{ t('No users found') }}</p>
        </div>

        <div v-else class="grid grid-cols-1 xl:grid-cols-2 gap-4">
          <article
            v-for="user in adminUsers"
            :key="user.id"
            class="rounded-[26px] p-5 text-white flex flex-col sm:flex-row sm:items-center justify-between gap-4"
            style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
          >
            <div class="min-w-0">
              <h2 class="text-base font-bold m-0 truncate">{{ user.display_name || user.email }}</h2>
              <p class="text-[11px] font-mono text-sky-200/80 m-0 mt-1 truncate">{{ user.email }}</p>
              <div class="flex items-center gap-2 mt-2">
                <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[9px] font-mono font-bold uppercase text-white">
                  {{ user.role }}
                </span>
                <span v-if="user.id === authUser?.id" class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[9px] font-mono font-bold uppercase text-white">
                  {{ t('You') }}
                </span>
              </div>
            </div>
            <button
              v-if="user.role === 'admin'"
              type="button"
              class="px-3.5 py-2 rounded-xl bg-black/35 hover:bg-black/50 text-white font-mono text-[11px] font-bold transition cursor-pointer disabled:opacity-50 flex-shrink-0"
              :disabled="isRoleUpdating || (user.id === authUser?.id && adminCount <= 1)"
              @click="updateUserRole(user, 'user')"
            >
              {{ t('Remove Admin') }}
            </button>
            <button
              v-else
              type="button"
              class="px-3.5 py-2 rounded-xl bg-white hover:bg-cyan-100 text-blue-900 font-mono text-[11px] font-bold transition cursor-pointer disabled:opacity-50 flex-shrink-0"
              :disabled="isRoleUpdating"
              @click="updateUserRole(user, 'admin')"
            >
              {{ t('Make Admin') }}
            </button>
          </article>
        </div>
      </div>

      <!-- Audit log -->
      <div v-else class="space-y-5">
        <div v-if="isAuditPending" class="grid grid-cols-1 gap-3">
          <div v-for="i in 4" :key="i" class="rounded-[22px] h-20 animate-pulse bg-white/[0.04]" />
        </div>

        <div
          v-else-if="!auditLog?.length"
          class="rounded-[26px] p-10 text-white text-center"
          style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.35) 55%, rgba(29, 78, 216, 0) 100%);"
        >
          <p class="text-base font-bold m-0">{{ t('No audit events yet.') }}</p>
        </div>

        <div v-else class="space-y-3">
          <article
            v-for="event in auditLog"
            :key="event.id"
            class="rounded-[22px] p-4 text-white flex flex-col sm:flex-row sm:items-center justify-between gap-3"
            style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.35) 55%, rgba(29, 78, 216, 0) 100%);"
          >
            <div class="min-w-0 space-y-1">
              <div class="flex flex-wrap items-center gap-2">
                <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[9px] font-mono font-bold uppercase text-white">
                  {{ event.action }}
                </span>
                <span class="px-2.5 py-1 rounded-full bg-black/55 border border-white/10 text-[9px] font-mono font-bold uppercase text-white">
                  {{ event.target_type }} {{ event.target_id ? `#${event.target_id}` : '' }}
                </span>
              </div>
              <p class="text-xs text-sky-200/85 m-0">
                {{ event.admin_email || event.admin_user_id }}
              </p>
            </div>
            <span class="text-[10px] font-mono text-sky-200/60 flex-shrink-0">{{ event.created_at }}</span>
          </article>
        </div>
      </div>
    </template>
  </div>
</template>
