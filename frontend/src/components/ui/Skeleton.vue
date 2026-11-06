<script setup lang="ts">
withDefaults(
  defineProps<{
    variant?: 'text' | 'card' | 'circle'
    width?: string
    height?: string
  }>(),
  {
    variant: 'text',
    width: 'w-full',
    height: 'h-4',
  }
)
</script>

<template>
  <div
    v-if="variant === 'text'"
    :class="[width, height]"
    class="bg-white/[0.04] animate-pulse rounded-lg"
  />
  <div
    v-else-if="variant === 'circle'"
    :class="[width || 'w-10', height || 'h-10']"
    class="bg-white/[0.04] animate-pulse rounded-full"
  />
  <div
    v-else
    class="w-full h-32 rounded-2xl bg-white/[0.03] animate-pulse p-4 space-y-3"
  >
    <div class="w-1/3 h-4 bg-white/[0.06] rounded" />
    <div class="w-2/3 h-8 bg-white/[0.08] rounded" />
    <div class="w-1/2 h-3 bg-white/[0.04] rounded" />
  </div>
</template>
