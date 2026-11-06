<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

interface DonutItem {
  key: string
  name: string
  count: number
  color: string
}

const props = defineProps<{
  items: DonutItem[]
  total: number
  unit: string
}>()

const isMounted = ref(false)
onMounted(() => {
  requestAnimationFrame(() => {
    isMounted.value = true
  })
})

const DONUT_R = 38
const DONUT_C = 2 * Math.PI * DONUT_R
const DONUT_GAP = 2.5

const safeTotal = computed(
  () => props.total || props.items.reduce((sum, item) => sum + item.count, 0) || 1
)

const segments = computed(() => {
  let accumulated = 0
  return props.items.map((item) => {
    const fraction = item.count / safeTotal.value
    const segment = {
      ...item,
      percent: Math.round(fraction * 100),
      len: Math.max(fraction * DONUT_C - DONUT_GAP, 0.5),
      offset: -accumulated * DONUT_C,
    }
    accumulated += fraction
    return segment
  })
})
</script>

<template>
  <div class="flex w-full flex-col xl:flex-row items-center justify-center gap-4 xl:gap-8">
    <div class="relative h-[180px] w-[180px] xl:h-[200px] xl:w-[200px] flex-shrink-0">
      <svg viewBox="0 0 100 100" class="w-full h-full -rotate-90">
        <circle cx="50" cy="50" :r="DONUT_R" fill="none" stroke="rgba(255,255,255,0.08)" stroke-width="12" />
        <circle
          v-for="segment in segments"
          :key="segment.key"
          cx="50"
          cy="50"
          :r="DONUT_R"
          fill="none"
          :stroke="segment.color"
          stroke-width="12"
          stroke-linecap="butt"
          :stroke-dasharray="`${isMounted ? segment.len : 0} ${DONUT_C}`"
          :stroke-dashoffset="segment.offset"
          class="transition-all duration-1000 ease-out"
        />
      </svg>
      <div class="absolute inset-0 flex flex-col items-center justify-center">
        <span class="text-3xl xl:text-4xl font-black font-mono leading-none">{{ total }}</span>
        <span class="text-[11px] xl:text-xs font-mono text-sky-200/90 mt-1">{{ unit }}</span>
      </div>
    </div>

    <div class="w-full max-w-[320px] xl:max-w-none xl:flex-1 space-y-2.5">
      <div v-for="segment in segments" :key="segment.key" class="flex items-center gap-2 min-w-0 text-xs">
        <span class="h-2 w-2 rounded-full flex-shrink-0" :style="{ background: segment.color }" />
        <span class="font-semibold text-white/90 truncate">{{ segment.name }}</span>
        <span class="ml-auto font-mono text-cyan-300 flex-shrink-0">
          {{ segment.count }} <span class="text-white/40">({{ segment.percent }}%)</span>
        </span>
      </div>
    </div>
  </div>
</template>
