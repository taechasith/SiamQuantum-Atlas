<script setup lang="ts">
import { computed } from 'vue'
import CountTo from '@/components/ui/CountTo.vue'
import QuantumCat from '@/components/visual/QuantumCat.vue'
import { useStatsSummaryQuery } from '@/api/queries'
import { lang } from '@/composables/useLocale'

const { data: stats } = useStatsSummaryQuery()
const isThai = computed(() => lang.value === 'th')
const yearRange = computed(() => {
  const range = stats.value?.year_range
  return range ? `${range[0]}—${range[1]}` : '—'
})

const copy = computed(() =>
  isThai.value
      ? {
        title: 'แผนที่ภูมิทัศน์ควอนตัมไทย',
        titleLines: ['แผนที่ภูมิทัศน์', 'ควอนตัมไทย'],
        secondary: 'เข้าสู่ Dashboard',
        stats: {
          sources: 'แหล่งข้อมูล',
          triplets: 'ชุด SRO',
          sites: 'พิกัดภูมิศาสตร์',
          span: 'ช่วงข้อมูล',
        },
        principlesTitle: 'ทำไมเราสร้าง Atlas นี้',
        principlesLead:
          'ข้อมูลควอนตัมของไทยกระจายอยู่ตามสื่อ มหาวิทยาลัย หน่วยงานรัฐ และวิดีโอออนไลน์ การมองภาพรวมจึงต้องเริ่มจากการเก็บข้อมูลอย่างเป็นระบบและรักษาร่องรอยที่มา',
        principles: [
          {
            title: 'Provenance first',
            description: 'ทุกข้อความและความสัมพันธ์เชื่อมกลับไปยังแหล่งข้อมูลต้นทางที่ตรวจสอบได้',
          },
          {
            title: 'Thai context',
            description: 'ออกแบบสำหรับสถานที่ สถาบัน และบริบทของระบบนิเวศควอนตัมไทยโดยเฉพาะ',
          },
          {
            title: 'Multiple lenses',
            description: 'มองได้ทั้งแผนที่ เครือข่าย แนวโน้ม และคลังข้อมูล จาก corpus เดียวกัน',
          },
        ],
        methodTitle: 'จากแหล่งข้อมูล สู่สัญญาณ',
        steps: [
          { title: 'Collect', description: 'รวบรวมข่าว งานวิจัย วิดีโอ และประกาศจากช่องทางที่หลากหลาย' },
          { title: 'Extract', description: 'สกัดความสัมพันธ์แบบ subject–relation–object และจัดหมวดหมู่โดเมน' },
          { title: 'Explore', description: 'สำรวจผ่านแผนที่ เครือข่าย สถิติ และคลังข้อมูล พร้อมหลักฐานต้นทาง' },
        ],
        ctaTitle: 'เริ่มจากแหล่งข้อมูล',
        ctaLead: 'เปิด Atlas แล้วเลือกมุมมองที่คุณต้องการ',
      }
    : {
        title: 'A living map of Thai quantum intelligence',
        titleLines: ['A living map of Thai', 'quantum intelligence'],
        secondary: 'Enter Dashboard',
        stats: {
          sources: 'Sources',
          triplets: 'SRO sets',
          sites: 'Geolocated sites',
          span: 'Time span',
        },
        principlesTitle: 'Why we built this Atlas',
        principlesLead:
          'Thai quantum signals are scattered across media, universities, government, and video. Seeing the bigger picture starts with structured collection and a traceable record of where every signal came from.',
        principles: [
          {
            title: 'Provenance first',
            description: 'Every claim and relationship links back to a source you can inspect.',
          },
          {
            title: 'Thai context',
            description: 'Built around the places, institutions, and dynamics of the Thai quantum ecosystem.',
          },
          {
            title: 'Multiple lenses',
            description: 'Move between maps, networks, trends, and the source library from one corpus.',
          },
        ],
        methodTitle: 'From source to signal',
        steps: [
          { title: 'Collect', description: 'Gather news, research, videos, and announcements across channels.' },
          { title: 'Extract', description: 'Extract subject–relation–object triplets and classify quantum domains.' },
          { title: 'Explore', description: 'Move through maps, networks, trends, and the source library with evidence intact.' },
        ],
        ctaTitle: 'Start with the source.',
        ctaLead: 'Open the Atlas and choose the lens you need.',
      }
)
</script>

<template>
  <div class="w-full pb-10">
    <section class="relative flex text-white">
      <div class="relative flex w-full flex-col justify-center gap-12 overflow-hidden rounded-[26px] px-6 pb-10 pt-6 lg:justify-between sm:px-10 sm:pb-12 sm:pt-8 md:min-h-[calc(100dvh-56px)] lg:pb-16 lg:pt-10" style="background: linear-gradient(180deg, #329dd2 0%, rgba(50, 157, 210, 0.45) 55%, rgba(50, 157, 210, 0) 100%);">
        <div class="relative z-10 hidden w-full pb-6 pt-0 md:pb-8 md:pt-0 lg:block">
          <div class="grid gap-6 lg:grid-cols-4 lg:gap-6">
            <div class="lg:col-start-3">
              <h2 class="text-2xl font-black tracking-tight sm:text-3xl">{{ copy.principlesTitle }}</h2>
              <p class="mt-6 text-sm leading-7 text-white/48">{{ copy.principlesLead }}</p>
            </div>
            <div class="space-y-4 lg:col-start-4">
              <div v-for="principle in copy.principles" :key="principle.title">
                <h3 class="text-base font-black">{{ principle.title }}</h3>
                <p class="mt-1 text-sm leading-6 text-white/48">{{ principle.description }}</p>
              </div>
            </div>
          </div>
        </div>

        <div class="relative z-10 max-w-[1500px] text-left">
          <h1 class="!text-[28px] sm:!text-[32px] md:!text-[clamp(2rem,2.73vw,3.255rem)] font-black leading-[0.92] tracking-[-0.05em]">
            <span v-for="(line, index) in copy.titleLines" :key="index" class="block">{{ line }}</span>
          </h1>
        </div>

        <div class="pointer-events-none absolute bottom-0 right-6 z-0 hidden h-[360px] w-[360px] lg:block xl:h-[420px] xl:w-[420px]">
          <div class="absolute inset-0">
            <QuantumCat height="100%" />
          </div>
        </div>
      </div>
    </section>

    <section class="relative pb-6 pt-8 md:pt-16 lg:hidden">
      <div class="relative mx-auto w-full max-w-[1100px]">
        <div class="relative h-[420px] overflow-hidden rounded-[26px] sm:h-[600px] lg:h-[760px]">
          <div class="absolute inset-0">
            <QuantumCat height="100%" />
          </div>
        </div>
      </div>
    </section>

    <section class="relative pb-10 pt-6">
      <div class="relative grid w-full grid-cols-2 gap-3 sm:gap-4 md:grid-rows-2 md:min-h-[calc(100dvh-56px)]">
        <article class="rounded-[22px] p-5 text-white flex flex-col items-start justify-center gap-2 text-left min-h-[124px]">
          <strong class="whitespace-nowrap !text-[36px] sm:!text-[clamp(3.5rem,7.5vw,9rem)] font-black font-mono text-white leading-none"><CountTo v-if="stats" :value="stats.total_sources" :duration="1000" /><span v-else>—</span></strong>
          <span class="!text-[10px] sm:!text-[clamp(1.5rem,3.2vw,3.75rem)] font-mono uppercase tracking-[0.12em] text-white leading-tight">{{ copy.stats.sources }}</span>
        </article>
        <article class="rounded-[22px] p-5 text-white flex flex-col items-start justify-center gap-2 text-left min-h-[124px]">
          <strong class="whitespace-nowrap !text-[36px] sm:!text-[clamp(3.5rem,7.5vw,9rem)] font-black font-mono text-white leading-none"><CountTo v-if="stats" :value="stats.total_triplets" format :duration="1200" /><span v-else>—</span></strong>
          <span class="!text-[10px] sm:!text-[clamp(1.5rem,3.2vw,3.75rem)] font-mono uppercase tracking-[0.12em] text-white leading-tight">{{ copy.stats.triplets }}</span>
        </article>
        <article class="rounded-[22px] p-5 text-white flex flex-col items-start justify-center gap-2 text-left min-h-[124px]">
          <strong class="whitespace-nowrap !text-[36px] sm:!text-[clamp(3.5rem,7.5vw,9rem)] font-black font-mono text-white leading-none"><CountTo v-if="stats" :value="stats.geo_count" :duration="900" /><span v-else>—</span></strong>
          <span class="!text-[10px] sm:!text-[clamp(1.5rem,3.2vw,3.75rem)] font-mono uppercase tracking-[0.12em] text-white leading-tight">{{ copy.stats.sites }}</span>
        </article>
        <article class="rounded-[22px] p-5 text-white flex flex-col items-start justify-center gap-2 text-left min-h-[124px]">
          <strong class="whitespace-nowrap !text-[28px] sm:!text-[clamp(3rem,6vw,7rem)] font-black font-mono text-white leading-none">{{ yearRange }}</strong>
          <span class="!text-[10px] sm:!text-[clamp(1.5rem,3.2vw,3.75rem)] font-mono uppercase tracking-[0.12em] text-white leading-tight">{{ copy.stats.span }}</span>
        </article>
      </div>
    </section>

    <section class="mt-6 grid gap-6 rounded-[26px] bg-white/[0.03] p-5 md:p-6 lg:hidden">
      <div>
        <h2 class="text-2xl font-black tracking-tight sm:text-3xl">{{ copy.principlesTitle }}</h2>
        <p class="mt-3 max-w-md text-sm leading-7 text-white/48">{{ copy.principlesLead }}</p>
      </div>
      <div>
        <div v-for="principle in copy.principles" :key="principle.title" class="py-4">
          <h3 class="text-base font-black">{{ principle.title }}</h3>
          <p class="mt-1 text-sm leading-6 text-white/48">{{ principle.description }}</p>
        </div>
      </div>
    </section>

    <section class="rounded-[26px] bg-[#080f1b] py-5">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 class="text-2xl font-black tracking-tight sm:text-3xl">{{ copy.methodTitle }}</h2>
        </div>
        <router-link to="/database" class="font-mono text-[10px] font-bold uppercase tracking-[0.14em] text-sky-200/70 transition hover:text-white">
          {{ isThai ? 'เปิดคลังข้อมูล' : 'Browse source library' }} ↗
        </router-link>
      </div>
      <div class="mt-6 grid gap-3 md:grid-cols-3">
        <div v-for="step in copy.steps" :key="step.title" class="rounded-2xl bg-white/[0.035] p-5">
          <h3 class="text-lg font-black">{{ step.title }}</h3>
          <p class="mt-1.5 text-sm leading-6 text-white/48">{{ step.description }}</p>
        </div>
      </div>
    </section>

    <section class="relative overflow-hidden rounded-[26px] bg-[linear-gradient(120deg,#329dd2_0%,#0a3a4f_55%,#080f1b_100%)] p-5 md:p-6">
      <div class="pointer-events-none absolute -right-12 -top-20 h-56 w-56 rounded-full bg-sky-300/15 blur-3xl" />
      <div class="relative flex flex-col gap-5 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 class="max-w-2xl text-2xl font-black tracking-tight sm:text-3xl">{{ copy.ctaTitle }}</h2>
          <p class="mt-2 text-sm text-white/55">{{ copy.ctaLead }}</p>
        </div>
        <router-link to="/dashboard" class="inline-flex flex-none items-center justify-center rounded-xl bg-[#ff4c2b] px-5 py-3 text-sm font-bold text-white transition hover:bg-[#e84324]">
          {{ copy.secondary }}
        </router-link>
      </div>
    </section>

    <p class="mt-6 px-2 text-center font-mono text-[9px] uppercase tracking-[0.14em] text-white/25">
      Boundaries: Natural Earth · GeoThai / OCHA COD-AB (CC BY 3.0 IGO)
    </p>
  </div>
</template>
