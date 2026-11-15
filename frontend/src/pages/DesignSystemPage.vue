<script setup lang="ts">
const colors = [
  { name: 'Siam Blue', hex: '#329DD2', role: 'Primary / data / links / focus', className: 'bg-[#329dd2]' },
  { name: 'Deep Navy', hex: '#080F1B', role: 'Page background / base surface', className: 'bg-[#080f1b]' },
  { name: 'Signal Orange', hex: '#FF4C2B', role: 'Primary CTA / critical highlight', className: 'bg-[#ff4c2b]' },
  { name: 'Dark Teal', hex: '#0A3A4F', role: 'Panels / secondary surface', className: 'bg-[#0a3a4f]' },
  { name: 'Text Primary', hex: '#F1F5F9', role: 'Headlines / primary copy', className: 'bg-[#f1f5f9]' },
  { name: 'Text Muted', hex: '#94A3B8', role: 'Supporting copy / metadata', className: 'bg-[#94a3b8]' },
  { name: 'Success', hex: '#10B981', role: 'Approved / positive state', className: 'bg-[#10b981]' },
  { name: 'Warning', hex: '#F59E0B', role: 'Attention / pending state', className: 'bg-[#f59e0b]' },
  { name: 'Danger', hex: '#F87171', role: 'Errors / destructive state', className: 'bg-[#f87171]' },
]

const typeScale = [
  {
    token: 'Display / Hero',
    font: 'IBM Plex Sans Thai',
    weight: '700 Bold',
    size: '56px desktop / 32px mobile',
    line: '1.0',
    tracking: '-0.04em',
    sample: 'A living map of Thai quantum intelligence',
    className: '!text-[32px] md:!text-[56px] font-bold leading-none tracking-[-0.04em] uppercase',
  },
  {
    token: 'Page H1',
    font: 'IBM Plex Sans Thai',
    weight: '700 Bold',
    size: '36px desktop / 24px mobile',
    line: '1.2',
    tracking: '-0.02em',
    sample: 'Quantum Corpus Database',
    className: 'text-2xl md:text-[36px] font-bold leading-tight tracking-[-0.02em] uppercase',
  },
  {
    token: 'H2',
    font: 'IBM Plex Sans Thai',
    weight: '700 Bold',
    size: '32–40px',
    line: '1.05',
    tracking: '-0.03em',
    sample: 'From source to signal',
    className: '!text-[28px] md:!text-[40px] font-bold leading-[1.1] tracking-[-0.03em]',
  },
  {
    token: 'H3',
    font: 'IBM Plex Sans Thai',
    weight: '600 Semibold',
    size: '28px desktop / 22px mobile',
    line: '1.2',
    tracking: '-0.02em',
    sample: 'Provenance first',
    className: 'text-[22px] md:text-[28px] font-semibold leading-[1.2] tracking-[-0.02em]',
  },
  {
    token: 'H4',
    font: 'IBM Plex Sans Thai',
    weight: '600 Semibold',
    size: '22px desktop / 18px mobile',
    line: '1.25',
    tracking: '-0.01em',
    sample: 'Source cluster detail',
    className: 'text-[18px] md:text-[22px] font-semibold leading-tight tracking-[-0.01em]',
  },
  {
    token: 'Body Large',
    font: 'IBM Plex Sans Thai',
    weight: '400 Regular',
    size: '16px',
    line: '1.65',
    tracking: '0',
    sample: 'Thai quantum signals are scattered across media, universities, government, and video.',
    className: '!text-base font-normal leading-[1.65]',
  },
  {
    token: 'Body',
    font: 'IBM Plex Sans Thai',
    weight: '400 Regular',
    size: '14px',
    line: '1.65',
    tracking: '0',
    sample: 'Every claim and relationship links back to a source you can inspect.',
    className: '!text-sm font-normal leading-[1.65]',
  },
  {
    token: 'Label / Eyebrow',
    font: 'JetBrains Mono',
    weight: '600 Semibold',
    size: '10–12px',
    line: '1.2',
    tracking: '0.18em',
    sample: 'CORPUS SOURCES · 164',
    className: '!text-[11px] font-mono font-semibold uppercase leading-tight tracking-[0.18em]',
  },
  {
    token: 'Caption',
    font: 'JetBrains Mono',
    weight: '400 Regular',
    size: '10px',
    line: '1.4',
    tracking: '0.12em',
    sample: 'Natural Earth · GeoThai / OCHA COD-AB',
    className: '!text-[10px] font-mono font-normal uppercase leading-snug tracking-[0.12em]',
  },
]
</script>

<template>
  <div class="mx-auto w-full max-w-6xl space-y-8 pb-12 text-white">
    <header class="border-b border-white/[0.08] pb-8">
      <p class="font-mono text-[10px] font-bold uppercase tracking-[0.22em] text-[#8fd5ec]/65">SiamQuantum Atlas · CI v1.0</p>
      <h1 class="mt-3 text-2xl md:text-[36px] font-bold uppercase leading-tight tracking-[-0.02em]">Design system</h1>
      <p class="mt-4 max-w-2xl text-sm leading-7 text-white/50">
        Typography, color, spacing, and interaction rules for the SiamQuantum Atlas product surface.
      </p>
    </header>

    <section class="space-y-5">
      <div>
        <p class="font-mono text-[10px] font-bold uppercase tracking-[0.2em] text-[#8fd5ec]/60">01 · Color</p>
        <h2 class="mt-2 !text-[28px] md:!text-[40px] font-bold leading-[1.1] tracking-[-0.03em]">Brand palette</h2>
      </div>
      <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        <article v-for="color in colors" :key="color.name" class="overflow-hidden rounded-2xl border border-white/[0.08] bg-white/[0.025]">
          <div class="h-24" :class="color.className" />
          <div class="space-y-1 p-4">
            <div class="flex items-center justify-between gap-3">
              <h3 class="text-sm font-bold">{{ color.name }}</h3>
              <span class="font-mono text-[10px] text-white/45">{{ color.hex }}</span>
            </div>
            <p class="text-xs leading-5 text-white/40">{{ color.role }}</p>
          </div>
        </article>
      </div>
    </section>

    <section class="space-y-5">
      <div>
        <p class="font-mono text-[10px] font-bold uppercase tracking-[0.2em] text-[#8fd5ec]/60">02 · Typography</p>
        <h2 class="mt-2 !text-[28px] md:!text-[40px] font-bold leading-[1.1] tracking-[-0.03em]">Type scale</h2>
      </div>
      <p class="font-mono text-[10px] uppercase tracking-[0.14em] text-white/35">Breakpoints: mobile &lt; 768px · desktop ≥ 768px · fixed values, no clamp</p>
      <div class="overflow-hidden rounded-2xl border border-white/[0.08] bg-white/[0.025]">
        <article v-for="item in typeScale" :key="item.token" class="grid gap-4 border-b border-white/[0.07] p-5 last:border-b-0 lg:grid-cols-[180px_1fr]">
          <div>
            <p class="font-mono text-[10px] font-bold uppercase tracking-[0.16em] text-[#8fd5ec]/65">{{ item.token }}</p>
            <p class="mt-2 font-mono text-[10px] leading-5 text-white/35">
              {{ item.font }}<br>
              {{ item.weight }} · {{ item.size }}<br>
              LH {{ item.line }} · LS {{ item.tracking }}
            </p>
          </div>
          <div class="flex min-h-[72px] items-center">
            <p :class="item.className">{{ item.sample }}</p>
          </div>
        </article>
      </div>
    </section>

    <section class="space-y-5">
      <div>
        <p class="font-mono text-[10px] font-bold uppercase tracking-[0.2em] text-[#8fd5ec]/60">03 · Components</p>
        <h2 class="mt-2 !text-[28px] md:!text-[40px] font-bold leading-[1.1] tracking-[-0.03em]">Controls</h2>
      </div>
      <div class="flex flex-wrap items-center gap-3 rounded-2xl border border-white/[0.08] bg-white/[0.025] p-5">
        <button type="button" class="rounded-xl bg-[#ff4c2b] px-5 py-3 text-sm font-bold text-white transition hover:bg-[#e84324]">Primary action</button>
        <button type="button" class="rounded-xl border border-white/10 bg-white/[0.04] px-5 py-3 text-sm font-semibold text-white/80 transition hover:bg-white/[0.08] hover:text-white">Secondary</button>
        <button type="button" class="rounded-xl px-5 py-3 text-sm font-semibold text-[#8fd5ec] transition hover:text-white">Ghost action</button>
        <span class="rounded-full bg-[#329dd2]/15 px-3 py-1.5 font-mono text-[10px] font-bold uppercase tracking-[0.16em] text-[#8fd5ec]">Label</span>
      </div>
    </section>

    <section class="space-y-4">
      <div>
        <p class="font-mono text-[10px] font-bold uppercase tracking-[0.2em] text-[#8fd5ec]/60">04 · Rules</p>
        <h2 class="mt-2 !text-[28px] md:!text-[40px] font-bold leading-[1.1] tracking-[-0.03em]">Usage</h2>
      </div>
      <div class="grid gap-3 md:grid-cols-2">
        <article class="rounded-2xl border border-emerald-300/15 bg-emerald-500/[0.06] p-5">
          <h3 class="text-sm font-bold text-emerald-300">Do</h3>
          <ul class="mt-3 space-y-2 text-sm leading-6 text-white/55">
            <li>Use 700 Bold as the heaviest weight; the loaded IBM Plex Sans Thai family stops at 700.</li>
            <li>Keep Signal Orange for one primary action or critical highlight per view.</li>
            <li>Use JetBrains Mono for numbers, metadata, labels, and technical IDs.</li>
            <li>Maintain a clear text hierarchy with one H1 per page.</li>
          </ul>
        </article>
        <article class="rounded-2xl border border-rose-300/15 bg-rose-500/[0.06] p-5">
          <h3 class="text-sm font-bold text-rose-300">Don’t</h3>
          <ul class="mt-3 space-y-2 text-sm leading-6 text-white/55">
            <li>Don’t use 800/900 weights or synthetic bold.</li>
            <li>Don’t use orange for body copy or large backgrounds.</li>
            <li>Don’t mix more than two type families in one component.</li>
            <li>Don’t use more than three competing accent colors in one section.</li>
          </ul>
        </article>
      </div>
    </section>

    <section class="space-y-4">
      <div>
        <p class="font-mono text-[10px] font-bold uppercase tracking-[0.2em] text-[#8fd5ec]/60">05 · Foundations</p>
        <h2 class="mt-2 !text-[28px] md:!text-[40px] font-bold leading-[1.1] tracking-[-0.03em]">Spacing & radius</h2>
      </div>
      <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <article v-for="item in [
          { label: 'Radius / Card', value: '26px' },
          { label: 'Radius / Control', value: '12–16px' },
          { label: 'Section gap', value: '24–32px' },
          { label: 'Base unit', value: '4px' },
        ]" :key="item.label" class="rounded-2xl border border-white/[0.08] bg-white/[0.025] p-5">
          <p class="font-mono text-[9px] font-bold uppercase tracking-[0.16em] text-white/35">{{ item.label }}</p>
          <p class="mt-2 text-xl font-bold">{{ item.value }}</p>
        </article>
      </div>
    </section>
  </div>
</template>
