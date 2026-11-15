<script setup lang="ts">
withDefaults(
  defineProps<{
    title?: string
    subtitle?: string
    customClass?: string
  }>(),
  {
    title: '',
    subtitle: '',
    customClass: '',
  }
)
</script>

<template>
  <div
    class="rounded-[30px] xl:rounded-[34px] p-5 sm:p-6 xl:p-[24px] text-white h-auto flex flex-col gap-4 select-none"
    :class="customClass"
    style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);"
  >
    <header v-if="title || $slots.header || $slots.icon" class="flex items-center justify-between flex-shrink-0">
      <div>
        <h2 class="flex items-center gap-2.5 text-xl xl:text-[22px] font-bold tracking-tight whitespace-nowrap text-white m-0">
          <slot name="icon">
            <svg class="w-5 h-5 xl:w-6 xl:h-6 text-white flex-none" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 1L9.6 6.4L15 8L9.6 9.6L8 15L6.4 9.6L1 8L6.4 6.4L8 1Z" />
            </svg>
          </slot>
          <span>{{ title }}</span>
        </h2>
        <p v-if="subtitle" class="text-sm text-sky-200/90 mt-1 m-0">{{ subtitle }}</p>
      </div>
      <slot name="header-right" />
    </header>
    <slot />
  </div>
</template>
