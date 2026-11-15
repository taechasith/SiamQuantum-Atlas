<script setup lang="ts">
withDefaults(
  defineProps<{
    title?: string
  }>(),
  {
    title: 'No Data Here',
  }
)
</script>

<template>
  <p class="w-full py-10 text-center font-mono text-xs font-bold uppercase tracking-[0.16em] text-white/55">
    {{ title }}
  </p>
</template>
