export type CatBreed = 'siamese' | 'khaoManee' | 'ginger' | 'quantum'

export const BREEDS: Record<CatBreed, {
  name: string
  label: string
  swatch: string
  colors: string[]
  body: number
  belly: number
  point: number
  muzzle: number
  earOuter: number
  earInner: number
  eyeIris: string
  eyeRim: string
  eyeCenter: string
  nose: number
  sheen: number
  isMetallic?: boolean
}> = {
  siamese: {
    name: 'แมววิเชียรมาศ',
    label: 'Siamese (Wichien Maat)',
    swatch: '#f6efe5',
    colors: ['#f6efe5', '#342013', '#00b4d8'],
    body: 0xf6efe5,      // Warm creamy ivory body
    belly: 0xfcf8f2,     // Pale cream underbelly
    point: 0x342013,     // Deep seal chocolate points
    muzzle: 0x3d2618,    // Soft chocolate muzzle
    earOuter: 0x2d1a0e,  // Chocolate ears
    earInner: 0xe5a296,  // Soft warm peach inner ear
    eyeIris: '#00b4d8',  // Vivid sapphire aquamarine
    eyeRim: '#0a3a4f',   // Deep royal navy limbal ring
    eyeCenter: '#caf0f8',// Sparkling crystal cyan core
    nose: 0xd9758c,      // Soft dusky rose leather
    sheen: 0xffffff,     // Velvet fur sheen
  },
  khaoManee: {
    name: 'แมวขาวมณี',
    label: 'Khao Manee (Jewel Eye)',
    swatch: '#ffffff',
    colors: ['#ffffff', '#f472b6', '#06b6d4'],
    body: 0xfcfdfd,      // Pure pearl white fur
    belly: 0xffffff,
    point: 0xf4f7fa,
    muzzle: 0xf6f9fb,
    earOuter: 0xfcfdfd,
    earInner: 0xfda4af,  // Rosy pink inner ears
    eyeIris: '#06b6d4',  // Brilliant crystal cyan
    eyeRim: '#0e7490',
    eyeCenter: '#a5f3fc',
    nose: 0xf472b6,      // Kitten pink nose
    sheen: 0xcfeef8,
  },
  ginger: {
    name: 'แมวส้มควอนตัม',
    label: 'Ginger Tabby (Flame)',
    swatch: '#eb8d43',
    colors: ['#eb8d43', '#fef3c7', '#eab308'],
    body: 0xeb8d43,      // Warm apricot ginger
    belly: 0xfef3c7,     // Cream bib & chest
    point: 0xc45f1b,     // Rich amber copper
    muzzle: 0xffedd5,    // Soft cream whisker pads
    earOuter: 0xd96b1e,
    earInner: 0xfecdd3,
    eyeIris: '#eab308',  // Glowing golden amber
    eyeRim: '#854d0e',
    eyeCenter: '#fef08a',
    nose: 0xfb7185,      // Coral pink
    sheen: 0xfef08a,
  },
  quantum: {
    name: 'ชเรอดิงเจอร์ โฮโล',
    label: 'Schrödinger Holo',
    swatch: '#1e293b',
    colors: ['#1e293b', '#329dd2', '#10b981'],
    body: 0x1e293b,      // Dark quantum obsidian
    belly: 0x334155,
    point: 0x0f172a,
    muzzle: 0x1e293b,
    earOuter: 0x0f172a,
    earInner: 0x38bdf8,
    eyeIris: '#10b981',  // Quantum radioactive emerald
    eyeRim: '#064e3b',
    eyeCenter: '#a7f3d0',
    nose: 0xf43f5e,      // Neon magenta
    sheen: 0x22d3ee,
    isMetallic: true,
  },
}
