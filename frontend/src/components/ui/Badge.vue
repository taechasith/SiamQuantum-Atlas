<script setup lang="ts">
withDefaults(
  defineProps<{
    variant?: 'neutral' | 'primary' | 'success' | 'warning' | 'danger' | 'wire'
    size?: 'sm' | 'md'
    dot?: boolean
  }>(),
  {
    variant: 'neutral',
    size: 'sm',
    dot: false,
  }
)

const variants = {
  neutral: 'bg-black/55 text-white border border-white/10',
  primary: 'bg-black/55 text-white border border-white/10',
  success: 'bg-black/55 text-white border border-white/10',
  warning: 'bg-black/55 text-white border border-white/10',
  danger: 'bg-black/55 text-white border border-white/10',
  wire: 'bg-black/55 text-white border border-white/10 hover:bg-black/70',
}

const sizes = {
  sm: 'text-[10px] px-2 py-0.5 rounded-full',
  md: 'text-xs px-2.5 py-1 rounded-full',
}
</script>

<template>
  <span
    class="inline-flex items-center gap-1.5 font-mono tracking-wider uppercase font-semibold transition"
    :class="[variants[variant], sizes[size]]"
  >
    <span
      v-if="dot"
      class="w-1.5 h-1.5 rounded-full"
      :class="{
        'bg-slate-400': variant === 'neutral' || variant === 'wire',
        'bg-sky-400': variant === 'primary',
        'bg-emerald-400': variant === 'success',
        'bg-amber-400': variant === 'warning',
        'bg-rose-400': variant === 'danger',
      }"
    />
    <slot />
  </span>
</template>
