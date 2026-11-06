<script setup lang="ts">
import { computed } from 'vue'
import AppTooltip from '@/components/ui/AppTooltip.vue'
import { t } from '@/composables/useLocale'

const props = withDefaults(
  defineProps<{
    label: string
    value: string | number
    trend?: string
    hint?: string
    info?: string
    color?: string
    spark?: number[]
    delay?: string
  }>(),
  {
    trend: '',
    hint: '',
    info: '',
    color: '#ffffff',
    spark: () => [10, 18, 25, 30, 45, 70, 95, 120, 170],
    delay: '0ms',
  }
)

const uniqueId = computed(() => {
  return props.label.toLowerCase().replace(/[^a-z0-9]/g, '-') || 'kpi'
})

const gradId = computed(() => `spark-grad-${uniqueId.value}`)

// Generate smooth cubic bezier curve for 100x36 SVG viewport
const pathData = computed(() => {
  const data = props.spark && props.spark.length >= 2
    ? props.spark
    : [10, 20, 30, 40, 50, 60]

  const width = 100
  const height = 36
  const paddingY = 4

  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1

  const points = data.map((val, i) => {
    const x = Number(((i / (data.length - 1)) * width).toFixed(2))
    const norm = (val - min) / range
    const y = Number(((height - paddingY) - norm * (height - paddingY * 2)).toFixed(2))
    return { x, y }
  })

  let linePath = `M ${points[0].x} ${points[0].y}`
  for (let i = 1; i < points.length; i++) {
    const prev = points[i - 1]
    const curr = points[i]
    const dx = curr.x - prev.x
    const cp1x = Number((prev.x + dx * 0.45).toFixed(2))
    const cp1y = prev.y
    const cp2x = Number((curr.x - dx * 0.45).toFixed(2))
    const cp2y = curr.y
    linePath += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${curr.x} ${curr.y}`
  }

  const lastPoint = points[points.length - 1]
  const areaPath = `${linePath} L ${width} ${height} L 0 ${height} Z`

  return { linePath, areaPath, lastPoint }
})
</script>

<template>
  <article
    class="card-fade group flex h-full flex-col justify-between p-3 sm:p-3.5 rounded-[24px] sm:rounded-[28px] overflow-hidden select-none transition-all duration-300 hover:scale-[1.01]"
    style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.45) 55%, rgba(29, 78, 216, 0) 100%);"
  >
    <!-- Top Information Section -->
    <div class="relative z-10">
      <!-- Header row: Label + Info Button + Trend Pill -->
      <div class="flex items-center justify-between gap-2">
        <h3 class="flex items-center gap-1.5 text-[12.5px] font-medium text-white/85 tracking-tight m-0 leading-none">
          <span>{{ label }}</span>
          <AppTooltip v-if="info" :content="info" side="top">
            <button
              type="button"
              class="inline-flex h-5 w-5 items-center justify-center rounded-full text-white/50 hover:text-white transition cursor-pointer"
              :aria-label="t('More information')"
            >
              <svg class="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
                <circle cx="12" cy="12" r="10" />
                <path d="M12 16v-4" />
                <path d="M12 8h.01" />
              </svg>
            </button>
          </AppTooltip>
        </h3>

        <!-- Trend Pill Badge with arrow -->
        <span
          v-if="trend"
          class="inline-flex items-center gap-0.5 rounded-full px-2 py-0.5 text-[11px] tracking-tight bg-black/55 text-white font-semibold font-mono"
        >
          <svg class="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M7 7h10v10" />
            <path d="M7 17 17 7" />
          </svg>
          {{ trend }}
        </span>
      </div>

      <!-- Main Value & Hint -->
      <div class="mt-2">
        <p class="tabular text-[24px] font-bold leading-tight tracking-[-0.03em] text-white sm:text-[26px] font-mono m-0">
          {{ value }}
        </p>
        <p v-if="hint" class="text-[11px] font-medium text-sky-200/80 m-0 mt-1 font-mono">
          {{ hint }}
        </p>
      </div>
    </div>

    <!-- Full-Width Bottom Smooth Sparkline with Area Gradient -->
    <div class="relative -mx-3 -mb-3 sm:-mx-3.5 sm:-mb-3.5 mt-2 overflow-hidden rounded-b-[24px]">
      <svg viewBox="0 0 100 36" fill="none" preserveAspectRatio="none" class="h-7 w-full" aria-hidden="true">
        <defs>
          <linearGradient :id="gradId" x1="0" y1="0" x2="0" y2="1">
            <stop stop-color="#ffffff" stop-opacity="0.25" />
            <stop offset="1" stop-color="#ffffff" stop-opacity="0" />
          </linearGradient>
        </defs>
        <g class="sparkline-reveal" :style="{ animationDelay: delay }">
          <!-- Area fill -->
          <path
            :d="pathData.areaPath"
            :fill="`url(#${gradId})`"
            class="sparkline-area"
            :style="{ animationDelay: delay }"
          />
          <!-- Stroke line -->
          <path
            :d="pathData.linePath"
            :stroke="color"
            stroke-width="1.8"
            stroke-linecap="round"
            stroke-linejoin="round"
            vector-effect="non-scaling-stroke"
          />
          <!-- Circle point at tip -->
          <circle
            :cx="pathData.lastPoint.x - 0.5"
            :cy="pathData.lastPoint.y"
            r="2"
            :fill="color"
            vector-effect="non-scaling-stroke"
            class="sparkline-dot"
            :style="{ animationDelay: delay ? `calc(${delay} + 0.75s)` : '0.75s' }"
          />
        </g>
      </svg>
    </div>
  </article>
</template>

<style scoped>
/* Running Sparkline Reveal Animation (Left-to-Right Draw) */
.sparkline-reveal {
  animation: revealSpark 0.95s cubic-bezier(0.16, 1, 0.3, 1) both;
}

@keyframes revealSpark {
  from {
    clip-path: inset(0 100% 0 0);
  }
  to {
    clip-path: inset(0 0% 0 0);
  }
}

.sparkline-area {
  animation: fadeInSparkArea 1.1s ease-out both;
}

@keyframes fadeInSparkArea {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}

.sparkline-dot {
  animation: popInDot 0.45s 0.75s cubic-bezier(0.34, 1.56, 0.64, 1) both;
  transform-origin: center;
  filter: drop-shadow(0 0 5px rgba(255, 255, 255, 0.95));
}

@keyframes popInDot {
  from {
    opacity: 0;
    transform: scale(0);
  }
  to {
    opacity: 1;
    transform: scale(1);
  }
}

@media (prefers-reduced-motion: reduce) {
  .sparkline-reveal,
  .sparkline-area,
  .sparkline-dot {
    animation: none !important;
  }
}
</style>
