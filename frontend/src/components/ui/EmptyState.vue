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
  <div class="mx-auto w-full max-w-2xl rounded-[22px] border border-white/10 bg-[#080f1b]/80 px-6 py-10 text-center shadow-[0_24px_80px_-40px_rgba(0,0,0,0.9)] backdrop-blur-sm">
    <div class="mx-auto flex h-14 w-14 items-center justify-center rounded-2xl border border-cyan-300/20 bg-cyan-300/10 text-cyan-300">
      <slot name="icon">
        <svg class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <circle cx="10.5" cy="10.5" r="6.5" stroke-width="1.6" />
          <path stroke-linecap="round" stroke-width="1.6" d="m15.5 15.5 4 4" />
          <path stroke-linecap="round" stroke-width="1.6" d="m8.5 8.5 4 4m0-4-4 4" />
        </svg>
      </slot>
    </div>
    <h4 class="mt-4 text-base font-black tracking-tight text-white">{{ title }}</h4>
    <p class="mx-auto mt-2 max-w-md text-xs leading-6 text-white/55">{{ message }}</p>
    <div v-if="$slots.actions" class="mt-5 flex justify-center">
      <slot name="actions" />
    </div>
  </div>
</template>
