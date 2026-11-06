<script setup lang="ts">
import { onMounted, onUnmounted, ref, watch } from 'vue'
import * as THREE from 'three'
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js'
import type { GeoCluster } from '@/lib/geo'
import { t } from '@/composables/useLocale'
import worldDots from '@/assets/world-dots-50m.json'
import thailandOutline from '@/assets/thailand-outline-50m.json'
import thailandProvinces from '@/assets/thailand-provinces-10m.json'

const props = withDefaults(
  defineProps<{
    clusters: GeoCluster[]
    selectedKey?: string | null
  }>(),
  {
    selectedKey: null,
  }
)

const emit = defineEmits<{
  (e: 'select-cluster', cluster: GeoCluster): void
}>()

const hostRef = ref<HTMLDivElement | null>(null)
const tooltip = ref({
  visible: false,
  x: 0,
  y: 0,
  label: '',
  count: 0,
})

const dots = worldDots as number[][]
const thaiRings = thailandOutline as number[][][]

interface ProvinceData {
  name: string
  nameLocal: string
  lat: number
  lng: number
  rings: number[][][]
}

const provinces = thailandProvinces as ProvinceData[]
const provinceLabelOverrides: Record<string, string> = {
  'Bangkok Metropolis': 'กรุงเทพมหานคร',
  Chaiyaphum: 'จังหวัดชัยภูมิ',
  'Bueng Kan': 'จังหวัดบึงกาฬ',
  Kanchanaburi: 'จังหวัดกาญจนบุรี',
  'Nakhon Pathom': 'จังหวัดนครปฐม',
  Phetchabun: 'จังหวัดเพชรบูรณ์',
}
const globeRadius = 1
const overlayRadius = globeRadius + 0.006
const markerRadius = overlayRadius + 0.001
const provinceLabelRadius = overlayRadius + 0.0004
const baseRotateSpeed = 0.55
const baseAutoRotateSpeed = 0.25
const baseZoomSpeed = 0.75
const referenceDistance = 4.1
const minCameraDistance = 1.03

let scene: THREE.Scene | null = null
let camera: THREE.PerspectiveCamera | null = null
let renderer: THREE.WebGLRenderer | null = null
let controls: OrbitControls | null = null
let markerGroup: THREE.Group | null = null
let markerTexture: THREE.Texture | null = null
let dotTexture: THREE.Texture | null = null
let provinceBoundaryMaterial: THREE.LineBasicMaterial | null = null
let subdistrictLines: THREE.LineSegments | null = null
let subdistrictMaterial: THREE.LineBasicMaterial | null = null
let subdistrictsLoaded = false
let subdistrictsLoading = false
let worldLinesLoaded = false
let worldLinesLoading = false
let worldCoastMaterial: THREE.LineBasicMaterial | null = null
let worldBorderMaterial: THREE.LineBasicMaterial | null = null
let disposed = false
let provinceLabelGroup: THREE.Group | null = null
interface ProvinceLabel {
  sprite: THREE.Sprite
  position: THREE.Vector3
  texture: THREE.Texture
  widthPx: number
  heightPx: number
  aspect: number
}
const provinceLabels: ProvinceLabel[] = []
const provinceLabelHeightPx = 14
let resizeObserver: ResizeObserver | null = null
let frameId = 0
let pointerDownAt: { x: number; y: number } | null = null
let autoRotateTimer: number | null = null
let cameraTween: {
  from: THREE.Vector3
  to: THREE.Vector3
  start: number
  duration: number
} | null = null
const raycaster = new THREE.Raycaster()
const pointer = new THREE.Vector2()
const prefersReducedMotion =
  typeof window !== 'undefined' &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches

function latLngToVector3(lat: number, lng: number, radius: number) {
  const phi = ((90 - lat) * Math.PI) / 180
  const theta = ((lng + 180) * Math.PI) / 180
  return new THREE.Vector3(
    -radius * Math.sin(phi) * Math.cos(theta),
    radius * Math.cos(phi),
    radius * Math.sin(phi) * Math.sin(theta)
  )
}

function makeRadialTexture(size: number, innerStop: number) {
  const canvas = document.createElement('canvas')
  canvas.width = size
  canvas.height = size
  const context = canvas.getContext('2d')
  if (!context) return new THREE.Texture()

  const center = size / 2
  const gradient = context.createRadialGradient(center, center, 0, center, center, center)
  gradient.addColorStop(0, 'rgba(255,255,255,1)')
  gradient.addColorStop(innerStop, 'rgba(255,255,255,0.95)')
  gradient.addColorStop(1, 'rgba(255,255,255,0)')
  context.fillStyle = gradient
  context.fillRect(0, 0, size, size)

  const texture = new THREE.CanvasTexture(canvas)
  texture.colorSpace = THREE.SRGBColorSpace
  texture.needsUpdate = true
  return texture
}

function makeMarkerTexture(size: number) {
  const canvas = document.createElement('canvas')
  canvas.width = size
  canvas.height = size
  const context = canvas.getContext('2d')
  if (!context) return new THREE.Texture()

  const center = size / 2
  context.beginPath()
  context.arc(center, center, size * 0.27, 0, Math.PI * 2)
  context.fillStyle = 'rgba(255,255,255,1)'
  context.fill()
  context.lineWidth = size * 0.035
  context.strokeStyle = 'rgba(255,255,255,0.92)'
  context.stroke()

  const texture = new THREE.CanvasTexture(canvas)
  texture.colorSpace = THREE.SRGBColorSpace
  texture.needsUpdate = true
  return texture
}

function disposeObject(object: THREE.Object3D) {
  object.traverse((child) => {
    const mesh = child as THREE.Mesh
    if (mesh.geometry) mesh.geometry.dispose()
    const material = (mesh as THREE.Mesh).material as
      | THREE.Material
      | THREE.Material[]
      | undefined
    if (Array.isArray(material)) {
      material.forEach((entry) => entry.dispose())
    } else if (material) {
      material.dispose()
    }
  })
}

function buildStars() {
  if (!scene) return

  const count = 1400
  const positions = new Float32Array(count * 3)

  for (let i = 0; i < count; i += 1) {
    const radius = 7 + Math.random() * 12
    const theta = Math.random() * Math.PI * 2
    const phi = Math.acos(2 * Math.random() - 1)
    positions[i * 3] = radius * Math.sin(phi) * Math.cos(theta)
    positions[i * 3 + 1] = radius * Math.cos(phi)
    positions[i * 3 + 2] = radius * Math.sin(phi) * Math.sin(theta)
  }

  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  const material = new THREE.PointsMaterial({
    color: 0xdbeafe,
    size: 0.018,
    transparent: true,
    opacity: 0.55,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
  })
  const stars = new THREE.Points(geometry, material)
  scene.add(stars)
}

function buildGraticule() {
  if (!scene) return

  const group = new THREE.Group()
  const material = new THREE.LineBasicMaterial({
    color: 0x60a5fa,
    transparent: true,
    opacity: 0.055,
    depthWrite: false,
  })

  for (let lat = -60; lat <= 60; lat += 30) {
    const points: THREE.Vector3[] = []
    for (let lng = -180; lng <= 180; lng += 3) {
      points.push(latLngToVector3(lat, lng, globeRadius + 0.002))
    }
    const geometry = new THREE.BufferGeometry().setFromPoints(points)
    group.add(new THREE.Line(geometry, material))
  }

  for (let lng = -180; lng < 180; lng += 30) {
    const points: THREE.Vector3[] = []
    for (let lat = -90; lat <= 90; lat += 3) {
      points.push(latLngToVector3(lat, lng, globeRadius + 0.002))
    }
    const geometry = new THREE.BufferGeometry().setFromPoints(points)
    group.add(new THREE.Line(geometry, material))
  }

  scene.add(group)
}

function buildLandDots() {
  if (!scene || !dotTexture) return

  const positions = new Float32Array(dots.length * 3)

  dots.forEach(([lat, lng], index) => {
    const point = latLngToVector3(lat, lng, globeRadius + 0.003)
    positions[index * 3] = point.x
    positions[index * 3 + 1] = point.y
    positions[index * 3 + 2] = point.z
  })

  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  const material = new THREE.PointsMaterial({
    color: 0x93c5fd,
    size: 2,
    map: dotTexture,
    transparent: true,
    opacity: 0.74,
    alphaTest: 0.1,
    sizeAttenuation: false,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
  })
  scene.add(new THREE.Points(geometry, material))
}

function buildSegments(
  segments: number[],
  radius: number,
  material: THREE.LineBasicMaterial
) {
  const vertices = new Float32Array((segments.length / 4) * 6)
  let cursor = 0

  for (let i = 0; i < segments.length; i += 4) {
    const p1 = latLngToVector3(segments[i], segments[i + 1], radius)
    const p2 = latLngToVector3(segments[i + 2], segments[i + 3], radius)
    vertices[cursor++] = p1.x
    vertices[cursor++] = p1.y
    vertices[cursor++] = p1.z
    vertices[cursor++] = p2.x
    vertices[cursor++] = p2.y
    vertices[cursor++] = p2.z
  }

  const geometry = new THREE.BufferGeometry()
  geometry.setAttribute('position', new THREE.BufferAttribute(vertices, 3))
  return new THREE.LineSegments(geometry, material)
}

function loadWorldLines() {
  if (!scene || worldLinesLoading || worldLinesLoaded) return
  worldLinesLoading = true

  import('@/assets/world-lines-50m.json').then(({ default: lines }) => {
    if (!scene || disposed) return

    worldCoastMaterial = new THREE.LineBasicMaterial({
      color: 0x93c5fd,
      transparent: true,
      opacity: 0.34,
      depthWrite: false,
    })
    worldBorderMaterial = new THREE.LineBasicMaterial({
      color: 0x60a5fa,
      transparent: true,
      opacity: 0.26,
      depthWrite: false,
    })

    scene.add(buildSegments(lines.coastline, globeRadius + 0.004, worldCoastMaterial))
    scene.add(buildSegments(lines.borders, globeRadius + 0.005, worldBorderMaterial))
    worldLinesLoaded = true
    worldLinesLoading = false
  }).catch(() => {
    worldLinesLoading = false
  })
}

function updateWorldLineVisibility() {
  if (!camera) return

  const zoomLevel = Math.min(
    1,
    Math.max(0, (camera.position.length() - minCameraDistance) / (referenceDistance - minCameraDistance))
  )
  if (worldCoastMaterial) worldCoastMaterial.opacity = 0.22 + zoomLevel * 0.16
  if (worldBorderMaterial) worldBorderMaterial.opacity = 0.16 + zoomLevel * 0.14
}

function buildThailandOutline() {
  if (!scene) return

  const material = new THREE.LineBasicMaterial({
    color: 0x93c5fd,
    transparent: true,
    opacity: 0.95,
    depthWrite: false,
  })

  for (const ring of thaiRings) {
    if (ring.length < 2) continue
    const points = ring.map(([lat, lng]) =>
      latLngToVector3(lat, lng, globeRadius + 0.008)
    )
    const geometry = new THREE.BufferGeometry().setFromPoints(points)
    scene.add(new THREE.LineLoop(geometry, material))
  }
}

function loadSubdistricts() {
  if (subdistrictsLoaded || subdistrictsLoading || !scene) return
  subdistrictsLoading = true

  import('@/assets/thailand-subdistricts.geojson.json').then(({ default: segments }) => {
    if (!scene) return

    const vertices = new Float32Array((segments.length / 4) * 6)
    let cursor = 0
    for (let i = 0; i < segments.length; i += 4) {
      const p1 = latLngToVector3(segments[i], segments[i + 1], globeRadius + 0.004)
      const p2 = latLngToVector3(segments[i + 2], segments[i + 3], globeRadius + 0.004)
      vertices[cursor++] = p1.x
      vertices[cursor++] = p1.y
      vertices[cursor++] = p1.z
      vertices[cursor++] = p2.x
      vertices[cursor++] = p2.y
      vertices[cursor++] = p2.z
    }

    const geometry = new THREE.BufferGeometry()
    geometry.setAttribute('position', new THREE.BufferAttribute(vertices, 3))
    subdistrictMaterial = new THREE.LineBasicMaterial({
      color: 0x7dd3fc,
      transparent: true,
      opacity: 0,
      depthWrite: false,
    })
    subdistrictLines = new THREE.LineSegments(geometry, subdistrictMaterial)
    scene.add(subdistrictLines)
    subdistrictsLoaded = true
    subdistrictsLoading = false
  }).catch(() => {
    subdistrictsLoading = false
  })
}

function updateSubdistrictVisibility() {
  if (!camera) return

  const distance = camera.position.length()
  if (distance < 1.72) loadSubdistricts()
  if (!subdistrictMaterial) return

  subdistrictMaterial.opacity = Math.min(0.2, Math.max(0, (1.72 - distance) / 0.38))
}

function buildProvinceBoundaries() {
  if (!scene) return

  provinceBoundaryMaterial = new THREE.LineBasicMaterial({
    color: 0x93c5fd,
    transparent: true,
    opacity: 0,
    depthWrite: false,
  })

  for (const province of provinces) {
    for (const ring of province.rings) {
      if (ring.length < 3) continue
      const points = ring.map(([lat, lng]) =>
        latLngToVector3(lat, lng, globeRadius + 0.006)
      )
      const geometry = new THREE.BufferGeometry().setFromPoints(points)
      scene.add(new THREE.LineLoop(geometry, provinceBoundaryMaterial))
    }
  }
}

function provinceCentroid(province: ProvinceData): [number, number] {
  let bestRing: number[][] | null = null
  let bestArea = 0

  for (const ring of province.rings) {
    if (ring.length < 3) continue
    let area2 = 0
    for (let i = 0; i < ring.length; i += 1) {
      const [latA, lngA] = ring[i]
      const [latB, lngB] = ring[(i + 1) % ring.length]
      area2 += lngA * latB - lngB * latA
    }
    if (Math.abs(area2) > bestArea) {
      bestArea = Math.abs(area2)
      bestRing = ring
    }
  }

  if (!bestRing || bestArea < 0.00001) {
    return [province.lat, province.lng]
  }

  let area2 = 0
  let cx = 0
  let cy = 0
  for (let i = 0; i < bestRing.length; i += 1) {
    const [latA, lngA] = bestRing[i]
    const [latB, lngB] = bestRing[(i + 1) % bestRing.length]
    const cross = lngA * latB - lngB * latA
    area2 += cross
    cx += (lngA + lngB) * cross
    cy += (latA + latB) * cross
  }

  if (Math.abs(area2) < 0.00001) {
    return [province.lat, province.lng]
  }

  return [cy / (3 * area2), cx / (3 * area2)]
}

function makeProvinceLabelTexture(text: string) {
  const fontSize = 64
  const padding = 14
  const canvas = document.createElement('canvas')
  const context = canvas.getContext('2d')
  if (!context) return { texture: new THREE.Texture(), aspect: 1 }

  context.font = `600 ${fontSize}px "Noto Sans Thai", ui-sans-serif, system-ui, sans-serif`
  const width = Math.ceil(context.measureText(text).width) + padding * 2
  const height = fontSize + padding * 2
  canvas.width = width
  canvas.height = height

  const renderContext = canvas.getContext('2d')
  if (!renderContext) return { texture: new THREE.Texture(), aspect: 1 }

  renderContext.font = `600 ${fontSize}px "Noto Sans Thai", ui-sans-serif, system-ui, sans-serif`
  renderContext.textAlign = 'center'
  renderContext.textBaseline = 'middle'
  renderContext.fillStyle = 'rgba(219, 234, 254, 0.96)'
  renderContext.shadowColor = 'rgba(2, 6, 23, 0.92)'
  renderContext.shadowBlur = 10
  renderContext.shadowOffsetY = 2
  renderContext.fillText(text, width / 2, height / 2)

  const texture = new THREE.CanvasTexture(canvas)
  texture.colorSpace = THREE.SRGBColorSpace
  texture.generateMipmaps = false
  texture.minFilter = THREE.LinearFilter
  texture.magFilter = THREE.LinearFilter
  texture.needsUpdate = true
  return { texture, aspect: width / height }
}

function updateProvinceLabelScales() {
  if (!renderer || !camera) return

  const viewportHeight = Math.max(renderer.domElement.clientHeight, 1)
  const halfFov = (camera.fov * Math.PI) / 360
  const scaleY = (provinceLabelHeightPx * 2 * Math.tan(halfFov)) / viewportHeight

  for (const label of provinceLabels) {
    label.sprite.scale.set(scaleY * label.aspect, scaleY, 1)
  }
}

function buildProvinceLabels() {
  if (!scene) return

  provinceLabelGroup = new THREE.Group()
  provinceLabelGroup.renderOrder = 3

  for (const province of provinces) {
    const text = (provinceLabelOverrides[province.name] || province.nameLocal || province.name).replace(/^จังหวัด/, '')
    const { texture, aspect } = makeProvinceLabelTexture(text)
    const [lat, lng] = provinceCentroid(province)
    const material = new THREE.SpriteMaterial({
      map: texture,
      transparent: true,
      depthTest: false,
      depthWrite: false,
      opacity: 0,
      sizeAttenuation: false,
    })
    const sprite = new THREE.Sprite(material)
    sprite.position.copy(latLngToVector3(lat, lng, provinceLabelRadius))
    sprite.renderOrder = 3
    sprite.visible = false
    provinceLabelGroup.add(sprite)
    provinceLabels.push({
      sprite,
      position: sprite.position.clone(),
      texture,
      widthPx: provinceLabelHeightPx * aspect,
      heightPx: provinceLabelHeightPx,
      aspect,
    })
  }

  scene.add(provinceLabelGroup)
  updateProvinceLabelScales()
}

function updateProvinceVisibility() {
  if (!camera) return

  const distance = camera.position.length()
  const boundaryOpacity = Math.min(0.42, Math.max(0, (2.4 - distance) / 0.5))
  if (provinceBoundaryMaterial) provinceBoundaryMaterial.opacity = boundaryOpacity

  const labelOpacity = Math.min(0.88, Math.max(0, (2.25 - distance) / 0.55))
  if (!labelOpacity || !renderer) {
    for (const label of provinceLabels) label.sprite.visible = false
    return
  }

  const viewportWidth = renderer.domElement.clientWidth
  const viewportHeight = renderer.domElement.clientHeight
  if (viewportWidth < 2 || viewportHeight < 2) return

  const viewDirection = camera.position.clone().normalize()
  const occupied: Array<{ left: number; right: number; top: number; bottom: number }> = []

  for (const label of provinceLabels) {
    const normal = label.position.clone().normalize()
    const projected = label.position.clone().project(camera)
    const x = (projected.x * 0.5 + 0.5) * viewportWidth
    const y = (-projected.y * 0.5 + 0.5) * viewportHeight
    const halfWidth = label.widthPx / 2 + 4
    const halfHeight = label.heightPx / 2 + 2
    const left = x - halfWidth
    const right = x + halfWidth
    const top = y - halfHeight
    const bottom = y + halfHeight

    const visible =
      normal.dot(viewDirection) > 0.18 &&
      projected.z > -1 &&
      projected.z < 1 &&
      right > 0 &&
      left < viewportWidth &&
      bottom > 0 &&
      top < viewportHeight

    if (!visible) {
      label.sprite.visible = false
      continue
    }

    const overlaps = occupied.some(
      (rect) => left < rect.right && right > rect.left && top < rect.bottom && bottom > rect.top
    )
    if (overlaps) {
      label.sprite.visible = false
      continue
    }

    occupied.push({ left, right, top, bottom })
    const material = label.sprite.material as THREE.SpriteMaterial
    material.opacity = labelOpacity
    label.sprite.visible = true
  }
}

function buildMarkers() {
  if (!scene || !markerTexture) return

  if (markerGroup) {
    scene.remove(markerGroup)
    disposeObject(markerGroup)
  }

  markerGroup = new THREE.Group()

  for (const cluster of props.clusters) {
    const position = latLngToVector3(cluster.lat, cluster.lng, markerRadius)
    const baseSize = Math.min(
      0.03,
      0.006 + Math.log10(cluster.items.length + 1) * 0.003
    )
    const markerColor = cluster.exact ? '#7dd3fc' : '#94a3b8'
    const material = new THREE.SpriteMaterial({
      map: markerTexture,
      color: new THREE.Color(markerColor),
      transparent: true,
      opacity: cluster.exact ? 1 : 0.7,
      sizeAttenuation: false,
      depthWrite: false,
      depthTest: true,
    })
    const sprite = new THREE.Sprite(material)
    sprite.position.copy(position)
    sprite.scale.set(baseSize * 2, baseSize * 2, 1)
    sprite.userData.cluster = cluster
    sprite.userData.baseScale = baseSize * 2
    markerGroup.add(sprite)
  }

  scene.add(markerGroup)
  applySelection()
}

function applySelection() {
  if (!markerGroup) return

  for (const child of markerGroup.children) {
    const sprite = child as THREE.Sprite
    const cluster = sprite.userData.cluster as GeoCluster | undefined
    if (!cluster) continue

    const baseScale = Number(sprite.userData.baseScale ?? 0.1)
    const selected = props.selectedKey === cluster.key
    const material = sprite.material as THREE.SpriteMaterial
    material.color.set(selected ? '#ffffff' : cluster.exact ? '#7dd3fc' : '#94a3b8')
    material.opacity = cluster.exact ? 1 : 0.7
    const scaleFactor = selected ? 1.25 : 1
    sprite.scale.set(baseScale * scaleFactor, baseScale * scaleFactor, 1)
  }
}

function animateCameraTo(lat: number, lng: number, distance: number, duration = 900) {
  if (!camera || !controls) return

  const to = latLngToVector3(lat, lng, distance)
  cameraTween = {
    from: camera.position.clone(),
    to,
    start: performance.now(),
    duration,
  }
  controls.autoRotate = false
}

function focusThailand() {
  animateCameraTo(15.87, 100.99, 2.05)
}

function resetView() {
  animateCameraTo(18, 80, 4.1)
}

function focusCluster(cluster: GeoCluster) {
  animateCameraTo(cluster.lat, cluster.lng, 2.05)
}

function updateMarkerVisibility() {
  if (!markerGroup || !camera) return

  const viewDirection = camera.position.clone().normalize()
  for (const child of markerGroup.children) {
    const sprite = child as THREE.Sprite
    sprite.visible = sprite.position.clone().normalize().dot(viewDirection) > 0.18
  }
}

function updateCameraTween(now: number) {
  if (!camera || !cameraTween) return

  const progress = Math.min(1, (now - cameraTween.start) / cameraTween.duration)
  const eased = 1 - Math.pow(1 - progress, 3)
  const fromDirection = cameraTween.from.clone().normalize()
  const toDirection = cameraTween.to.clone().normalize()
  const direction = fromDirection.lerp(toDirection, eased)

  if (direction.lengthSq() < 0.0001) direction.copy(toDirection)
  direction.normalize()

  const distance =
    cameraTween.from.length() * (1 - eased) + cameraTween.to.length() * eased
  camera.position.copy(direction.multiplyScalar(distance))

  if (progress >= 1) {
    cameraTween = null
    scheduleAutoRotate()
  }
}

function scheduleAutoRotate() {
  if (prefersReducedMotion || !controls) return

  if (autoRotateTimer) window.clearTimeout(autoRotateTimer)
  autoRotateTimer = window.setTimeout(() => {
    if (controls && !props.selectedKey) controls.autoRotate = true
  }, 5000)
}

function pauseAutoRotate() {
  if (autoRotateTimer) window.clearTimeout(autoRotateTimer)
  if (controls) controls.autoRotate = false
  cameraTween = null
}

function updatePointer(event: PointerEvent) {
  if (!renderer) return

  const rect = renderer.domElement.getBoundingClientRect()
  pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1
  pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1
}

function pickCluster(event: PointerEvent) {
  if (!camera || !markerGroup) return null

  updatePointer(event)
  raycaster.setFromCamera(pointer, camera)
  const hits = raycaster.intersectObjects(markerGroup.children, false)

  for (const hit of hits) {
    const cluster = hit.object.userData.cluster as GeoCluster | undefined
    if (!cluster) continue
    const normal = hit.object.position.clone().normalize()
    const viewDirection = camera.position.clone().normalize()
    if (normal.dot(viewDirection) < 0.18) continue
    return cluster
  }

  return null
}

function onPointerDown(event: PointerEvent) {
  pointerDownAt = { x: event.clientX, y: event.clientY }
  pauseAutoRotate()
  tooltip.value.visible = false
}

function onPointerMove(event: PointerEvent) {
  if (!renderer) return

  const cluster = pickCluster(event)
  if (cluster && !pointerDownAt) {
    const rect = renderer.domElement.getBoundingClientRect()
    tooltip.value = {
      visible: true,
      x: event.clientX - rect.left,
      y: event.clientY - rect.top,
      label: cluster.label,
      count: cluster.items.length,
    }
    renderer.domElement.style.cursor = 'pointer'
  } else {
    tooltip.value.visible = false
    renderer.domElement.style.cursor = pointerDownAt ? 'grabbing' : 'grab'
  }
}

function onPointerUp(event: PointerEvent) {
  const start = pointerDownAt
  pointerDownAt = null

  if (start) {
    const distance = Math.hypot(event.clientX - start.x, event.clientY - start.y)
    if (distance < 5) {
      const cluster = pickCluster(event)
      if (cluster) {
        emit('select-cluster', cluster)
        return
      }
    }
  }

  scheduleAutoRotate()
}

function onPointerLeave() {
  pointerDownAt = null
  tooltip.value.visible = false
  scheduleAutoRotate()
}

function onWheel() {
  pauseAutoRotate()
  scheduleAutoRotate()
}

function resize() {
  if (!renderer || !camera || !hostRef.value) return

  const width = hostRef.value.clientWidth || window.innerWidth
  const height = hostRef.value.clientHeight || window.innerHeight
  renderer.setSize(width, height, false)
  camera.aspect = width / Math.max(height, 1)
  camera.updateProjectionMatrix()
  updateProvinceLabelScales()
}

function updateControlSpeed() {
  if (!camera || !controls) return

  const distance = camera.position.length()
  const zoomLevel = Math.min(
    1,
    Math.max(0, (distance - minCameraDistance) / (referenceDistance - minCameraDistance))
  )
  const closeGrip = Math.pow(1 - zoomLevel, 1.5)
  const rotateFactor = 0.04 + 0.96 * Math.pow(zoomLevel, 1.8)
  controls.rotateSpeed = baseRotateSpeed * rotateFactor
  controls.autoRotateSpeed = baseAutoRotateSpeed * rotateFactor
  controls.zoomSpeed = baseZoomSpeed * (0.18 + 0.82 * zoomLevel)
  controls.dampingFactor = 0.06 + closeGrip * 0.12
}

function animate(now: number) {
  if (!renderer || !scene || !camera || !controls) return

  frameId = requestAnimationFrame(animate)
  updateCameraTween(now)
  updateControlSpeed()
  controls.update()
  updateProvinceVisibility()
  updateSubdistrictVisibility()
  updateWorldLineVisibility()
  updateMarkerVisibility()

  renderer.render(scene, camera)
}

function init() {
  if (!hostRef.value) return

  scene = new THREE.Scene()
  scene.background = new THREE.Color('#000000')

  camera = new THREE.PerspectiveCamera(42, 1, 0.01, 60)
  camera.position.copy(latLngToVector3(18, 80, 4.1))

  renderer = new THREE.WebGLRenderer({
    antialias: true,
    alpha: false,
    powerPreference: 'high-performance',
  })
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2))
  renderer.outputColorSpace = THREE.SRGBColorSpace
  renderer.domElement.style.cursor = 'grab'
  renderer.domElement.setAttribute('role', 'img')
  renderer.domElement.setAttribute(
    'aria-label',
    'Interactive 3D globe showing geolocated Thai quantum sources'
  )
  renderer.domElement.style.touchAction = 'none'
  hostRef.value.appendChild(renderer.domElement)

  controls = new OrbitControls(camera, renderer.domElement)
  controls.enablePan = false
  controls.enableDamping = true
  controls.dampingFactor = 0.06
  controls.minDistance = minCameraDistance
  controls.maxDistance = 5.2
  controls.rotateSpeed = baseRotateSpeed
  controls.zoomSpeed = baseZoomSpeed
  controls.autoRotateSpeed = baseAutoRotateSpeed
  controls.autoRotate = !prefersReducedMotion

  dotTexture = makeRadialTexture(32, 0.35)
  markerTexture = makeMarkerTexture(128)

  const ambient = new THREE.AmbientLight(0x3b82f6, 0.75)
  const keyLight = new THREE.DirectionalLight(0xffffff, 1.25)
  keyLight.position.set(3, 2, 4)
  const rimLight = new THREE.PointLight(0x3b82f6, 2.2, 8)
  rimLight.position.set(-3, -1, 2)
  scene.add(ambient, keyLight, rimLight)

  const globe = new THREE.Mesh(
    new THREE.SphereGeometry(globeRadius, 96, 96),
    new THREE.MeshPhongMaterial({
      color: 0x030b1a,
      emissive: 0x0b1f4a,
      emissiveIntensity: 0.72,
      shininess: 18,
      specular: 0x0f2a4a,
    })
  )
  scene.add(globe)

  const atmosphere = new THREE.Mesh(
    new THREE.SphereGeometry(globeRadius * 1.105, 64, 64),
    new THREE.ShaderMaterial({
      vertexShader: `
        varying vec3 vNormal;
        void main() {
          vNormal = normalize(normalMatrix * normal);
          gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        }
      `,
      fragmentShader: `
        varying vec3 vNormal;
        void main() {
          float intensity = pow(0.66 - dot(vNormal, vec3(0.0, 0.0, 1.0)), 2.6);
          gl_FragColor = vec4(0.16, 0.43, 0.98, 1.0) * intensity;
        }
      `,
      transparent: true,
      side: THREE.BackSide,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    })
  )
  scene.add(atmosphere)

  buildStars()
  buildGraticule()
  buildLandDots()
  buildThailandOutline()
  loadWorldLines()
  buildProvinceBoundaries()
  buildProvinceLabels()
  buildMarkers()

  resizeObserver = new ResizeObserver(resize)
  resizeObserver.observe(hostRef.value)
  resize()

  renderer.domElement.addEventListener('pointerdown', onPointerDown)
  renderer.domElement.addEventListener('pointermove', onPointerMove)
  renderer.domElement.addEventListener('pointerup', onPointerUp)
  renderer.domElement.addEventListener('pointerleave', onPointerLeave)
  renderer.domElement.addEventListener('wheel', onWheel, { passive: true })

  frameId = requestAnimationFrame(animate)
}

onMounted(init)

watch(
  () => props.clusters,
  () => buildMarkers()
)

watch(
  () => props.selectedKey,
  () => {
    applySelection()
    const selected = props.clusters.find((cluster) => cluster.key === props.selectedKey)
    if (selected) focusCluster(selected)
    else scheduleAutoRotate()
  }
)

onUnmounted(() => {
  disposed = true
  cancelAnimationFrame(frameId)
  if (autoRotateTimer) window.clearTimeout(autoRotateTimer)
  resizeObserver?.disconnect()
  controls?.dispose()

  if (renderer) {
    renderer.domElement.removeEventListener('pointerdown', onPointerDown)
    renderer.domElement.removeEventListener('pointermove', onPointerMove)
    renderer.domElement.removeEventListener('pointerup', onPointerUp)
    renderer.domElement.removeEventListener('pointerleave', onPointerLeave)
    renderer.domElement.removeEventListener('wheel', onWheel)
    renderer.dispose()
    renderer.domElement.remove()
  }

  if (scene) {
    disposeObject(scene)
    scene.clear()
  }

  for (const label of provinceLabels) label.texture.dispose()
  markerTexture?.dispose()
  dotTexture?.dispose()
})

defineExpose({
  focusThailand,
  resetView,
})
</script>

<template>
  <div class="relative h-full w-full overflow-hidden bg-black">
    <div ref="hostRef" class="absolute inset-0" />
    <div
      v-if="tooltip.visible"
      class="pointer-events-none absolute z-20 -translate-x-1/2 -translate-y-[calc(100%+12px)] rounded-xl border border-sky-300/20 bg-black/85 px-3 py-2 text-left shadow-2xl backdrop-blur-md"
      :style="{ left: `${tooltip.x}px`, top: `${tooltip.y}px` }"
    >
      <p class="text-xs font-bold text-white">{{ tooltip.label }}</p>
      <p class="mt-0.5 font-mono text-[10px] text-sky-200/80">
        {{ tooltip.count }} {{ t(tooltip.count === 1 ? 'source' : 'sources') }}
      </p>
    </div>
  </div>
</template>

