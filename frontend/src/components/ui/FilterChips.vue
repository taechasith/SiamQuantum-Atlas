<script setup lang="ts">
interface ChipOption {
  value: string
  label: string
}

defineProps<{
  modelValue: string
  options: ChipOption[]
}>()

const emit = defineEmits<{ 'update:modelValue': [value: string] }>()
</script>

<template>
  <div class="flex flex-wrap gap-2">
    <button
      v-for="option in options"
      :key="option.value"
      type="button"
      class="rounded-full px-3 py-1.5 font-mono text-[10px] font-bold tracking-[0.08em] transition"
      :class="modelValue === option.value
        ? 'bg-[#ff4c2b] text-white'
        : 'bg-white/[0.06] text-white/60 hover:bg-white/[0.1] hover:text-white'"
      @click="emit('update:modelValue', option.value)"
    >
      {{ option.label }}
    </button>
  </div>
</template>
