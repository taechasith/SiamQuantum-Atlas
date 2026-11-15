<script setup lang="ts">
interface SegmentOption {
  value: string
  label: string
}

defineProps<{
  modelValue: string
  options: SegmentOption[]
  ariaLabel?: string
}>()

const emit = defineEmits<{
  'update:modelValue': [value: string]
}>()
</script>

<template>
  <div
    class="flex items-center overflow-hidden rounded-full border border-white/[0.08] bg-white/[0.06]"
    role="group"
    :aria-label="ariaLabel"
  >
    <button
      v-for="option in options"
      :key="option.value"
      type="button"
      :aria-pressed="modelValue === option.value"
      class="whitespace-nowrap rounded-full px-3 py-1.5 font-mono text-[11px] font-bold uppercase tracking-wider transition cursor-pointer"
      :class="modelValue === option.value ? 'bg-[#ff4c2b] text-white' : 'text-slate-300 hover:text-white'"
      @click="emit('update:modelValue', option.value)"
    >
      {{ option.label }}
    </button>
  </div>
</template>
