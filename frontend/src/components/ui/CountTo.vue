<script setup lang="ts">
import { ref, watch, onMounted, onUnmounted } from 'vue'

const props = withDefaults(
  defineProps<{
    value: number
    duration?: number
    delay?: number
    format?: boolean
    prefix?: string
    suffix?: string
  }>(),
  {
    duration: 1200,
    delay: 50,
    format: false,
    prefix: '',
    suffix: '',
  }
)

const displayValue = ref(0)
let animationFrameId: number | null = null

const startAnimation = (target: number) => {
  if (animationFrameId !== null) {
    cancelAnimationFrame(animationFrameId)
  }

  const startVal = displayValue.value
  const diff = target - startVal
  if (diff === 0) {
    displayValue.value = target
    return
  }

  const startTime = performance.now() + props.delay

  const step = (now: number) => {
    if (now < startTime) {
      animationFrameId = requestAnimationFrame(step)
      return
    }

    const elapsed = now - startTime
    const progress = Math.min(elapsed / props.duration, 1)

    // Ease-out cubic: 1 - (1 - p)^3
    const ease = 1 - Math.pow(1 - progress, 3)
    displayValue.value = Math.round(startVal + diff * ease)

    if (progress < 1) {
      animationFrameId = requestAnimationFrame(step)
    } else {
      displayValue.value = target
      animationFrameId = null
    }
  }

  animationFrameId = requestAnimationFrame(step)
}

onMounted(() => {
  startAnimation(props.value)
})

watch(
  () => props.value,
  (newVal) => {
    startAnimation(newVal)
  }
)

onUnmounted(() => {
  if (animationFrameId !== null) {
    cancelAnimationFrame(animationFrameId)
  }
})
</script>

<template>
  <span class="inline-block tabular-nums">
    {{ prefix }}{{ format ? displayValue.toLocaleString('en-US') : displayValue }}{{ suffix }}
  </span>
</template>
