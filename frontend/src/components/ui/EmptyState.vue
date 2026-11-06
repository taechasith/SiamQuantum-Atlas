<script setup lang="ts">
withDefaults(
  defineProps<{
    title?: string
    message?: string
  }>(),
  {
    title: 'No Data Found',
    message: 'There are currently no records matching your criteria.',
  }
)
</script>

<template>
  <div class="rounded-3xl p-8 py-12 w-full text-center space-y-3 bg-white/[0.02] shadow-inner">
    <div class="w-12 h-12 rounded-2xl bg-white/[0.05] mx-auto flex items-center justify-center text-slate-400 shadow-md">
      <slot name="icon">
        <svg class="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
        </svg>
      </slot>
    </div>
    <h4 class="text-sm font-bold text-white">{{ title }}</h4>
    <p class="text-xs text-slate-400 max-w-sm mx-auto leading-relaxed">{{ message }}</p>
    <div v-if="$slots.actions" class="pt-2">
      <slot name="actions" />
    </div>
  </div>
</template>
