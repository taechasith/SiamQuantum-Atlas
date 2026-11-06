<script setup lang="ts">
import { ref, computed } from 'vue'
import { useGeoListQuery, type GeoItem } from '@/api/queries'
import GeoMap from '@/components/map/GeoMap.vue'

const { data: geoPoints } = useGeoListQuery('th')

const searchQuery = ref('')
const selectedPlatform = ref('all')
const selectedPoint = ref<GeoItem | null>(null)

const defaultItem: GeoItem = {
  source_id: 1886,
  title: 'โลกกำลังเข้าสู่ยุคควอนตัม และนี่คือโอกาสของโคราช!',
  city: 'Nakhon Ratchasima',
  region: 'Nakhon Ratchasima',
  platform: 'web',
  lat: 14.97,
  lng: 102.1,
  isp: 'Cloudflare',
  url: 'https://techsauce.co/tech-and-biz/korat-quantum-technology-hub-asean-economy',
}

const filteredPoints = computed(() => {
  if (!geoPoints.value) return []
  return geoPoints.value.filter((pt) => {
    const matchSearch =
      !searchQuery.value ||
      pt.title?.toLowerCase().includes(searchQuery.value.toLowerCase()) ||
      pt.city?.toLowerCase().includes(searchQuery.value.toLowerCase()) ||
      pt.isp?.toLowerCase().includes(searchQuery.value.toLowerCase())

    const matchPlatform =
      selectedPlatform.value === 'all' || pt.platform === selectedPlatform.value

    return matchSearch && matchPlatform
  })
})

const currentItem = computed<GeoItem>(() => {
  return selectedPoint.value || filteredPoints.value.find((p) => p.source_id === 1886) || filteredPoints.value[0] || defaultItem
})

function onSelectPoint(pt: GeoItem) {
  selectedPoint.value = pt
}
</script>

<template>
  <div class="h-full w-full relative overflow-hidden bg-[#000000]">
    <!-- Fullscreen Map (Zero Padding, Zero Gap, Zero Border) -->
    <GeoMap
      :points="filteredPoints"
      :selectedId="currentItem.source_id"
      height="100%"
      class="w-full h-full"
      @select-point="onSelectPoint"
    />
  </div>
</template>
