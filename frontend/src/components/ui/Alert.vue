<script setup lang="ts">
withDefaults(
  defineProps<{
    variant?: 'info' | 'success' | 'warning' | 'danger'
    title?: string
    dismissible?: boolean
  }>(),
  {
    variant: 'info',
    dismissible: false,
  }
)

const variants = {
  info: 'bg-sky-950/40 text-sky-200 shadow-sm',
  success: 'bg-emerald-950/40 text-emerald-200 shadow-sm',
  warning: 'bg-amber-950/40 text-amber-200 shadow-sm',
  danger: 'bg-rose-950/40 text-rose-200 shadow-sm',
}
</script>

<template>
  <div
    class="p-4 text-xs rounded-2xl w-full backdrop-blur-md transition-all shadow-md"
    :class="variants[variant]"
  >
    <div class="flex items-start justify-between gap-3">
      <div class="space-y-1">
        <h5 v-if="title" class="font-bold tracking-tight text-white flex items-center gap-2">
          <span class="w-2 h-2 rounded-full" :class="{
            'bg-sky-400': variant === 'info',
            'bg-emerald-400': variant === 'success',
            'bg-amber-400': variant === 'warning',
            'bg-rose-400': variant === 'danger'
          }" />
          {{ title }}
        </h5>
        <div class="leading-relaxed opacity-90">
          <slot />
        </div>
      </div>
    </div>
  </div>
</template>
