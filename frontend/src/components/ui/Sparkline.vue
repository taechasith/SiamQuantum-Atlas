<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    values: number[]
    variant?: 'squares' | 'line'
    width?: number
    height?: number
    color?: string
  }>(),
  {
    variant: 'squares',
    width: 90,
    height: 28,
    color: '#38BDF8',
  }
)

const columns = computed(() => {
  const vals = props.values.length ? props.values : [1, 2, 3, 2, 4, 3, 5, 4, 6]
  const max = Math.max(...vals, 1)
  return vals.map((v) => Math.max(1, Math.round((v / max) * 4))) // 1 to 4 squares height
})
</script>

<template>
  <div class="inline-flex items-end gap-1 h-7">
    <!-- Matrix Squares Variant (Huuk Signature) -->
    <div
      v-for="(level, idx) in columns"
      :key="idx"
      class="flex flex-col gap-0.5 justify-end h-full"
    >
      <div
        v-for="dot in 4"
        :key="dot"
        class="w-1.5 h-1 rounded-[1px] transition-all duration-300"
        :class="[
          5 - dot <= level ? 'opacity-90' : 'opacity-15',
        ]"
        :style="{ backgroundColor: color }"
      />
    </div>
  </div>
</template>
