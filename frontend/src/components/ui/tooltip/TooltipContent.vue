<script setup lang="ts">
import type { HTMLAttributes } from 'vue'
import { reactiveOmit } from '@vueuse/core'
import {
  TooltipContent,
  type TooltipContentEmits,
  type TooltipContentProps,
  TooltipPortal,
  useForwardPropsEmits,
} from 'reka-ui'
import { cn } from '@/lib/utils'

defineOptions({ inheritAttrs: false })

const props = withDefaults(
  defineProps<TooltipContentProps & { class?: HTMLAttributes['class'] }>(),
  {
    sideOffset: 8,
  }
)

const emits = defineEmits<TooltipContentEmits>()
const delegatedProps = reactiveOmit(props, 'class')
const forwarded = useForwardPropsEmits(delegatedProps, emits)
</script>

<template>
  <TooltipPortal>
    <TooltipContent
      v-bind="{ ...forwarded, ...$attrs }"
      :class="
        cn(
          'tooltip-content z-[9999] max-w-[min(20rem,calc(100vw-1rem))] rounded-xl border border-slate-200 bg-white px-3 py-2 text-left text-xs font-sans leading-relaxed text-slate-900 shadow-xl shadow-black/25 outline-none',
          props.class
        )
      "
    >
      <slot />
    </TooltipContent>
  </TooltipPortal>
</template>

<style scoped>
.tooltip-content {
  transform-origin: var(--reka-tooltip-content-transform-origin, center);
  animation: tooltip-in 140ms cubic-bezier(0.16, 1, 0.3, 1);
}

@keyframes tooltip-in {
  from {
    opacity: 0;
    transform: scale(0.96);
  }
  to {
    opacity: 1;
    transform: scale(1);
  }
}
</style>
