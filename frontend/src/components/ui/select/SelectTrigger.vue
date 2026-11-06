<script setup lang="ts">
import type { HTMLAttributes } from 'vue'
import { SelectIcon, SelectTrigger, type SelectTriggerProps } from 'reka-ui'
import { ChevronDown } from 'lucide-vue-next'
import { cn } from '@/lib/utils'

const props = defineProps<SelectTriggerProps & { class?: HTMLAttributes['class'] }>()
</script>

<template>
  <SelectTrigger
    v-bind="props"
    :class="cn(
      'flex h-10 w-full items-center justify-between gap-2 rounded-xl bg-black/35 px-3.5 text-xs font-mono text-white outline-none transition hover:bg-black/45 focus:ring-2 focus:ring-cyan-300/40 disabled:cursor-not-allowed disabled:opacity-50 data-[placeholder]:text-white/40',
      props.class
    )"
  >
    <slot />
    <SelectIcon as-child>
      <ChevronDown class="h-3.5 w-3.5 flex-shrink-0 opacity-60" />
    </SelectIcon>
  </SelectTrigger>
</template>
