<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import type { GeoItem } from '@/api/queries'

const props = withDefaults(
  defineProps<{
    points: GeoItem[]
    selectedId?: number | null
    height?: number | string
  }>(),
  {
    selectedId: null,
    height: 480,
  }
)

const emit = defineEmits<{
  (e: 'select-point', point: GeoItem): void
  (e: 'select-source', point: GeoItem): void
}>()

const mapContainer = ref<HTMLDivElement | null>(null)
let mapInstance: any = null
let markersLayer: any = null

async function initMap() {
  if (!mapContainer.value) return

  const L = (await import('leaflet')).default

  if (mapInstance) {
    mapInstance.remove()
  }

  // Centered on Thailand
  mapInstance = L.map(mapContainer.value, {
    zoomControl: false,
    attributionControl: false,
  }).setView([14.5, 100.5], 6)

  // High-performance clean dark basemap without watermarks (Esri World Dark Gray Base)
  L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}', {
    maxZoom: 16,
    attribution: 'Esri, HERE, Garmin, © OpenStreetMap',
  }).addTo(mapInstance)

  // Add zoom control at bottom right
  L.control.zoom({ position: 'bottomright' }).addTo(mapInstance)

  mapInstance.on('zoomend', () => renderMarkers(L))

  renderMarkers(L)
}

function spreadLatLng(lat: number, lng: number, index: number) {
  const zoom = mapInstance?.getZoom() ?? 10
  const angle = index * Math.PI * (3 - Math.sqrt(5))
  const radiusPx = 18 + Math.sqrt(index + 1) * 11
  const metersPerPixel =
    (156543.03392 * Math.cos((lat * Math.PI) / 180)) / Math.pow(2, zoom)
  const radiusMeters = radiusPx * metersPerPixel

  return [
    lat + (Math.sin(angle) * radiusMeters) / 111_320,
    lng + (Math.cos(angle) * radiusMeters) / (111_320 * Math.cos((lat * Math.PI) / 180)),
  ] as [number, number]
}

function renderMarkers(L: any) {
  if (!mapInstance) return

  if (markersLayer) {
    mapInstance.removeLayer(markersLayer)
  }

  markersLayer = L.layerGroup().addTo(mapInstance)

  const groups = new Map<string, GeoItem[]>()
  props.points.forEach((pt) => {
    if (!pt.lat || !pt.lng) return
    const key = `${pt.lat.toFixed(5)},${pt.lng.toFixed(5)}`
    const group = groups.get(key)
    if (group) group.push(pt)
    else groups.set(key, [pt])
  })

  const zoom = mapInstance.getZoom()

  groups.forEach((group) => {
    const representative = group.find((item) => !item.is_approximate) ?? group[0]
    if (!representative) return

    const count = group.length
    const approximate = group.every((item) => item.is_approximate)
    const color = approximate ? '251,191,36' : '56,189,248'

    // At high zoom, fan every record out around its shared coordinate so all
    // overlapping records are individually visible and clickable.
    if (zoom >= 14 && count > 1) {
      group.forEach((item, index) => {
        const [lat, lng] = spreadLatLng(representative.lat, representative.lng, index)
        const icon = L.divIcon({
          className: 'custom-geo-marker expanded-geo-marker',
          html: `
            <div class="cursor-pointer" style="width:14px;height:14px;border-radius:9999px;background:rgb(${color});box-shadow:0 0 9px rgba(${color},0.85);border:1.5px solid rgba(7,11,23,0.9);transition:transform 120ms ease" onmouseover="this.style.transform='scale(1.45)'" onmouseout="this.style.transform='scale(1)'"></div>
          `,
          iconSize: [14, 14],
          iconAnchor: [7, 7],
        })
        const marker = L.marker([lat, lng], { icon })
        marker.on('click', () => emit('select-source', item))
        marker.addTo(markersLayer)
      })
      return
    }

    // At normal zoom, one marker per shared coordinate with a record count.
    const badge = count > 1
      ? `<span class="absolute -right-4 -top-4 min-w-[20px] rounded-full border border-black/50 bg-[#07111f] px-1.5 font-mono text-[11px] font-bold leading-5 text-white shadow-lg">${count > 999 ? '999+' : count}</span>`
      : ''

    const customIcon = L.divIcon({
      className: 'custom-geo-marker',
      html: `
        <div class="relative group cursor-pointer" style="--marker-rgb:${color}">
          <div style="width:20px;height:20px;border-radius:9999px;background:rgb(${color});box-shadow:0 0 18px rgba(${color},0.95);border:2px solid rgba(7,11,23,0.9);transition:transform 160ms ease" class="group-hover:scale-125"></div>
          <div style="position:absolute;top:-4px;left:-4px;width:26px;height:26px;border-radius:9999px;background:rgba(${color},0.18)" class="animate-ping"></div>
          ${badge}
        </div>
      `,
      iconSize: [22, 22],
      iconAnchor: [11, 11],
    })

    const marker = L.marker([representative.lat, representative.lng], { icon: customIcon })
    marker.on('click', () => {
      emit('select-point', representative)
    })
    marker.addTo(markersLayer)
  })
}

watch(
  () => props.points,
  async () => {
    const L = (await import('leaflet')).default
    renderMarkers(L)
  },
  { deep: true }
)

onMounted(() => {
  initMap()
  setTimeout(() => {
    if (mapInstance) mapInstance.invalidateSize()
  }, 200)
})

onUnmounted(() => {
  if (mapInstance) {
    mapInstance.remove()
  }
})
</script>

<template>
  <div class="relative w-full h-full rounded-3xl overflow-hidden bg-slate-950 shadow-2xl">
    <div
      ref="mapContainer"
      class="w-full h-full z-0"
      :style="{ height: typeof height === 'number' ? `${height}px` : height }"
    />
  </div>
</template>

<style>
.custom-geo-marker {
  background: transparent;
  border: none;
}
.leaflet-container {
  background-color: #070B17 !important;
}
</style>
