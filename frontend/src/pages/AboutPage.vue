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
  return range ? `${range[0]}—${range[1]}` : '2016—2026'
})

const copy = computed(() =>
  isThai.value
      ? {
        title: 'แผนที่ภูมิทัศน์ควอนตัมไทย',
        primary: 'เปิด Map & Graph',
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
        methodEyebrow: 'How it works',
        methodTitle: 'จากแหล่งข้อมูล สู่สัญญาณ',
        steps: [
          { number: '01', title: 'Collect', description: 'รวบรวมข่าว งานวิจัย วิดีโอ และประกาศจากช่องทางที่หลากหลาย' },
          { number: '02', title: 'Extract', description: 'สกัดความสัมพันธ์แบบ subject–relation–object และจัดหมวดหมู่โดเมน' },
          { number: '03', title: 'Explore', description: 'สำรวจผ่านแผนที่ เครือข่าย สถิติ และคลังข้อมูล พร้อมหลักฐานต้นทาง' },
        ],
        ctaTitle: 'เริ่มจากแหล่งข้อมูล จบที่สัญญาณที่ชัดเจน',
        ctaLead: 'เปิด Atlas แล้วเลือกมุมมองที่คุณต้องการ',
      }
    : {
        title: 'A living map of Thai quantum intelligence',
        primary: 'Open Map & Graph',
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
        methodEyebrow: 'How it works',
        methodTitle: 'From source to signal',
        steps: [
          { number: '01', title: 'Collect', description: 'Gather news, research, videos, and announcements across channels.' },
          { number: '02', title: 'Extract', description: 'Extract subject–relation–object triplets and classify quantum domains.' },
          { number: '03', title: 'Explore', description: 'Move through maps, networks, trends, and the source library with evidence intact.' },
        ],
        ctaTitle: 'Start with the source. End with the signal.',
        ctaLead: 'Open the Atlas and choose the lens you need.',
      }
)
</script>

<template>
  <div class="mx-auto w-full max-w-[1320px] space-y-6 pb-10">
    <section
      class="relative overflow-hidden rounded-[30px] border border-sky-200/15 p-5 text-white sm:p-7 lg:p-9"
      style="background: linear-gradient(135deg, #1d4ed8 0%, #0b2a68 55%, #020617 100%);"
    >
      <div class="pointer-events-none absolute -right-20 -top-28 h-80 w-80 rounded-full bg-sky-300/15 blur-3xl" />
      <div class="relative grid items-center gap-8 lg:grid-cols-[1.05fr_.95fr] lg:gap-12">
        <div>
          <h1 class="max-w-2xl text-3xl font-black leading-[0.98] tracking-[-0.035em] sm:text-5xl lg:text-[3.7rem]">
            {{ copy.title }}
          </h1>
          <div class="mt-6 flex flex-wrap gap-3">
            <router-link to="/atlas" class="rounded-xl bg-white px-5 py-3 text-sm font-bold text-slate-950 transition hover:bg-sky-100">
              {{ copy.primary }} ↗
            </router-link>
            <router-link to="/dashboard" class="rounded-xl border border-white/15 bg-white/[0.04] px-5 py-3 text-sm font-semibold text-white/80 transition hover:border-white/30 hover:bg-white/[0.08] hover:text-white">
              {{ copy.secondary }}
            </router-link>
          </div>
        </div>

        <div class="relative">
          <div class="relative h-[300px] overflow-hidden rounded-[26px] border border-white/10 bg-[radial-gradient(circle_at_50%_38%,rgba(56,189,248,.2),transparent_46%)] sm:h-[380px] lg:h-[430px]">
            <div class="absolute inset-0">
              <QuantumCat height="100%" />
            </div>
          </div>
          <div class="absolute -bottom-4 -left-4 hidden rounded-2xl border border-white/10 bg-[#071126]/90 px-4 py-3 shadow-2xl backdrop-blur-xl sm:block">
            <p class="font-mono text-[9px] uppercase tracking-[0.18em] text-white/35">{{ copy.stats.sources }}</p>
            <p class="mt-1 font-mono text-2xl font-black text-white"><CountTo :value="stats?.total_sources ?? 170" :duration="1200" /></p>
          </div>
        </div>
      </div>
    </section>

    <section class="grid grid-cols-2 gap-3 rounded-[24px] border border-white/10 bg-white/[0.03] p-3 md:grid-cols-4">
      <div class="rounded-2xl bg-black/25 px-4 py-4">
        <p class="font-mono text-[9px] uppercase tracking-[0.18em] text-white/35">{{ copy.stats.sources }}</p>
        <p class="mt-1.5 font-mono text-2xl font-black"><CountTo :value="stats?.total_sources ?? 170" :duration="1000" /></p>
      </div>
      <div class="rounded-2xl bg-black/25 px-4 py-4">
        <p class="font-mono text-[9px] uppercase tracking-[0.18em] text-white/35">{{ copy.stats.triplets }}</p>
        <p class="mt-1.5 font-mono text-2xl font-black"><CountTo :value="stats?.total_triplets ?? 2090" format :duration="1200" /></p>
      </div>
      <div class="rounded-2xl bg-black/25 px-4 py-4">
        <p class="font-mono text-[9px] uppercase tracking-[0.18em] text-white/35">{{ copy.stats.sites }}</p>
        <p class="mt-1.5 font-mono text-2xl font-black"><CountTo :value="stats?.geo_count ?? 36" :duration="900" /></p>
      </div>
      <div class="rounded-2xl bg-black/25 px-4 py-4">
        <p class="font-mono text-[9px] uppercase tracking-[0.18em] text-white/35">{{ copy.stats.span }}</p>
        <p class="mt-1.5 font-mono text-xl font-black">{{ yearRange }}</p>
      </div>
    </section>

    <section class="grid gap-6 rounded-[26px] border border-white/10 bg-white/[0.03] p-6 md:p-8 lg:grid-cols-[.8fr_1.2fr] lg:gap-14">
      <div>
        <h2 class="text-2xl font-black tracking-tight sm:text-3xl">{{ copy.principlesTitle }}</h2>
        <p class="mt-3 max-w-md text-sm leading-7 text-white/48">{{ copy.principlesLead }}</p>
      </div>
      <div class="divide-y divide-white/10 border-y border-white/10">
        <div v-for="(principle, index) in copy.principles" :key="principle.title" class="grid grid-cols-[2.5rem_1fr] gap-3 py-5">
          <span class="font-mono text-[10px] tracking-[0.2em] text-sky-200/45">0{{ index + 1 }}</span>
          <div>
            <h3 class="text-base font-black">{{ principle.title }}</h3>
            <p class="mt-1 text-sm leading-6 text-white/48">{{ principle.description }}</p>
          </div>
        </div>
      </div>
    </section>

    <section class="rounded-[26px] border border-white/10 bg-[#040914] p-6 md:p-8">
      <div class="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p class="font-mono text-[10px] uppercase tracking-[0.2em] text-sky-200/60">{{ copy.methodEyebrow }}</p>
          <h2 class="mt-2 text-2xl font-black tracking-tight sm:text-3xl">{{ copy.methodTitle }}</h2>
        </div>
        <router-link to="/database" class="font-mono text-[10px] font-bold uppercase tracking-[0.14em] text-sky-200/70 transition hover:text-white">
          {{ isThai ? 'เปิดคลังข้อมูล' : 'Browse source library' }} ↗
        </router-link>
      </div>
      <div class="mt-6 grid gap-3 md:grid-cols-3">
        <div v-for="step in copy.steps" :key="step.number" class="rounded-2xl border border-white/8 bg-white/[0.035] p-5">
          <span class="font-mono text-[10px] tracking-[0.2em] text-sky-200/45">{{ step.number }}</span>
          <h3 class="mt-4 text-lg font-black">{{ step.title }}</h3>
          <p class="mt-1.5 text-sm leading-6 text-white/48">{{ step.description }}</p>
        </div>
      </div>
    </section>

    <section class="relative overflow-hidden rounded-[26px] border border-sky-200/15 bg-[linear-gradient(120deg,#1d4ed8_0%,#0b2a68_55%,#020617_100%)] px-6 py-8 sm:px-9">
      <div class="pointer-events-none absolute -right-12 -top-20 h-56 w-56 rounded-full bg-sky-300/15 blur-3xl" />
      <div class="relative flex flex-col gap-5 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 class="max-w-2xl text-2xl font-black tracking-tight sm:text-3xl">{{ copy.ctaTitle }}</h2>
          <p class="mt-2 text-sm text-white/55">{{ copy.ctaLead }}</p>
        </div>
        <router-link to="/dashboard" class="inline-flex flex-none items-center justify-center rounded-xl bg-white px-5 py-3 text-sm font-bold text-slate-950 transition hover:bg-sky-100">
          {{ copy.secondary }}
        </router-link>
      </div>
    </section>

    <p class="px-2 text-center font-mono text-[9px] uppercase tracking-[0.14em] text-white/25">
      Boundaries: Natural Earth · GeoThai / OCHA COD-AB (CC BY 3.0 IGO)
    </p>
  </div>
</template>
