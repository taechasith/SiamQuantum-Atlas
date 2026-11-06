<script setup lang="ts">
import type { HTMLAttributes } from 'vue'
import { SelectContent, type SelectContentProps, SelectPortal, SelectViewport } from 'reka-ui'
import { cn } from '@/lib/utils'

const props = withDefaults(
  defineProps<SelectContentProps & { class?: HTMLAttributes['class'] }>(),
  {
    position: 'popper',
    sideOffset: 6,
  }
)
</script>

<template>
  <SelectPortal>
    <SelectContent
      v-bind="props"
      :class="cn(
        'relative z-[2600] max-h-[min(22rem,var(--reka-select-content-available-height))] min-w-[10rem] overflow-hidden rounded-xl border border-white/10 bg-[#0b1324] text-white shadow-2xl',
        props.class
      )"
    >
      <SelectViewport class="p-1">
        <slot />
      </SelectViewport>
    </SelectContent>
  </SelectPortal>
</template>
