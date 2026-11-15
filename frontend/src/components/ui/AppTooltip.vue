<script setup lang="ts">
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'

withDefaults(
  defineProps<{
    content?: string
    side?: 'top' | 'right' | 'bottom' | 'left'
    disabled?: boolean
  }>(),
  {
    content: '',
    side: 'top',
    disabled: false,
  }
)
</script>

<template>
  <Tooltip v-if="content && !disabled">
    <TooltipTrigger as-child>
      <slot />
    </TooltipTrigger>
    <TooltipContent :side="side">
      {{ content }}
    </TooltipContent>
  </Tooltip>
  <slot v-else />
</template>
