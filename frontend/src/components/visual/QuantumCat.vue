<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'

const props = withDefaults(
  defineProps<{
    height?: number | string
  }>(),
  {
    height: '100%',
  }
)

const containerRef = ref<HTMLDivElement | null>(null)
let animId: number | null = null
let renderer: any = null
let resizeObserver: ResizeObserver | null = null
let resizeFrame: number | null = null

async function initRobotCat() {
  const wrap = containerRef.value
  if (!wrap) return

  const THREE = await import('three')

  const W = wrap.clientWidth || 440
  const H = wrap.clientHeight || 440

  const scene = new THREE.Scene()

  // Perfectly framed for the 4-legged sitting Chibi Mecha Cat
  const camera = new THREE.PerspectiveCamera(30, W / H, 0.1, 50)
  camera.position.set(0, 0.12, 3.25)
  camera.lookAt(0, 0.10, 0)
  const baseAspect = W / H
  const baseCameraZ = camera.position.z

  renderer = new THREE.WebGLRenderer({
    antialias: true,
    alpha: true,
    powerPreference: 'high-performance',
  })
  renderer.setSize(W, H, false)
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
  renderer.domElement.style.width = '100%'
  renderer.domElement.style.height = '100%'
  renderer.domElement.style.display = 'block'
  renderer.outputColorSpace = THREE.SRGBColorSpace
  renderer.toneMapping = THREE.NoToneMapping
  renderer.shadowMap.enabled = false
  wrap.appendChild(renderer.domElement)

  // ── SOFT MODERN STUDIO LIGHTING (CI BLUE ACCENTED) ─────────────────────
  const hemi = new THREE.HemisphereLight(0xffffff, 0x0a3a4f, 1.35)
  hemi.position.set(0, 3, 0)
  scene.add(hemi)

  const keyLight = new THREE.DirectionalLight(0xffffff, 1.15)
  keyLight.position.set(1.8, 2.8, 3.2)
  scene.add(keyLight)

  const fillLight = new THREE.DirectionalLight(0x8fd5ec, 0.75)
  fillLight.position.set(-2.2, 1.4, 1.8)
  scene.add(fillLight)

  // ── CRISP MODERN OLED VISOR TEXTURE (1024x512) ──────────────────────────
  const faceCanvas = document.createElement('canvas')
  faceCanvas.width = 1024
  faceCanvas.height = 512
  const fctx = faceCanvas.getContext('2d')!

  function drawVisorFace(
    blinkAmount: number,
    pulsePhase: number,
    lookX = 0,
    lookY = 0
  ) {
    const w = faceCanvas.width
    const h = faceCanvas.height
    fctx.clearRect(0, 0, w, h)

    const padX = 58
    const padY = 42
    const vw = w - padX * 2
    const vh = h - padY * 2
    const cornerRadius = 185

    // Outer Visor Rim
    fctx.save()
    fctx.fillStyle = '#272a3a'
    fctx.beginPath()
    fctx.roundRect(padX, padY, vw, vh, cornerRadius)
    fctx.fill()

    // Inner OLED Screen
    const inPad = 16
    fctx.beginPath()
    fctx.roundRect(
      padX + inPad,
      padY + inPad,
      vw - inPad * 2,
      vh - inPad * 2,
      cornerRadius - inPad
    )
    fctx.clip()

    // Pure Deep Matte Black OLED Screen
    fctx.fillStyle = '#080f1b'
    fctx.fillRect(0, 0, w, h)

    // Soft top ambient reflection
    fctx.fillStyle = 'rgba(255, 255, 255, 0.07)'
    fctx.beginPath()
    fctx.ellipse(w * 0.5, padY + 20, vw * 0.42, 68, 0, 0, Math.PI * 2)
    fctx.fill()

    const eyeShiftX = lookX * 30
    const eyeShiftY = lookY * 20
    const pupilShiftX = lookX * 12
    const pupilShiftY = lookY * 9

    // Glowing Electric Cyan / Sky Blue Eyes (CI Blue)
    const eyeCenters = [
      { x: w * 0.31 + eyeShiftX, y: h * 0.46 + eyeShiftY },
      { x: w * 0.69 + eyeShiftX, y: h * 0.46 + eyeShiftY },
    ]
    const eyeW = 132
    const eyeH = Math.max(12, 146 * (1 - blinkAmount))

    eyeCenters.forEach((ec, idx) => {
      fctx.save()
      fctx.translate(ec.x, ec.y)
      fctx.rotate(idx === 0 ? 0.05 : -0.05)

      const rx = eyeW / 2
      const ry = eyeH / 2
      const cornerR = Math.min(rx, ry, 62)

      fctx.shadowColor = '#329dd2'
      fctx.shadowBlur = 18

      fctx.fillStyle = '#329dd2'
      fctx.beginPath()
      fctx.roundRect(-rx, -ry, eyeW, eyeH, cornerR)
      fctx.fill()

      if (blinkAmount < 0.6) {
        fctx.shadowBlur = 0
        fctx.fillStyle = '#080f1b'
        fctx.beginPath()
        fctx.arc(
          pupilShiftX,
          2 + pupilShiftY,
          34 * (1 - blinkAmount * 0.5),
          0,
          Math.PI * 2
        )
        fctx.fill()

        // Crisp White Catchlight
        fctx.fillStyle = '#ffffff'
        fctx.beginPath()
        fctx.ellipse(
          -24 + pupilShiftX * 0.4,
          -26 + pupilShiftY * 0.4,
          20,
          15,
          -0.25,
          0,
          Math.PI * 2
        )
        fctx.fill()

        fctx.beginPath()
        fctx.arc(22 + pupilShiftX * 0.3, 24 + pupilShiftY * 0.3, 8.5, 0, Math.PI * 2)
        fctx.fill()
      }
      fctx.restore()
    })

    // Soft Sky Blue Blush Marks
    const blushCenters = [
      { x: w * 0.175 + eyeShiftX * 0.55, y: h * 0.65 + eyeShiftY * 0.55 },
      { x: w * 0.825 + eyeShiftX * 0.55, y: h * 0.65 + eyeShiftY * 0.55 },
    ]
    blushCenters.forEach((bc) => {
      fctx.save()
      fctx.translate(bc.x, bc.y)
      fctx.strokeStyle = '#329dd2'
      fctx.lineWidth = 7
      fctx.lineCap = 'round'
      for (let i = -1; i <= 1; i++) {
        fctx.beginPath()
        fctx.moveTo(i * 17 - 7, 11)
        fctx.lineTo(i * 17 + 7, -11)
        fctx.stroke()
      }
      fctx.restore()
    })

    // Cute ':3' (ω) Mouth & Nose (Sky Cyan Accent)
    fctx.save()
    fctx.translate(w * 0.5 + eyeShiftX * 0.7, h * 0.62 + eyeShiftY * 0.7)

    fctx.fillStyle = '#329dd2'
    fctx.beginPath()
    fctx.moveTo(-10, -17)
    fctx.lineTo(10, -17)
    fctx.lineTo(0, -5)
    fctx.closePath()
    fctx.fill()

    fctx.strokeStyle = '#329dd2'
    fctx.lineWidth = 7.5
    fctx.lineCap = 'round'
    fctx.lineJoin = 'round'
    fctx.beginPath()
    fctx.arc(-21, 5, 21, 0.15 * Math.PI, 0.92 * Math.PI)
    fctx.stroke()
    fctx.beginPath()
    fctx.arc(21, 5, 21, 0.08 * Math.PI, 0.85 * Math.PI)
    fctx.stroke()
    fctx.restore()

    fctx.restore()
  }

  drawVisorFace(0, 0, 0, 0)
  const visorTexture = new THREE.CanvasTexture(faceCanvas)
  visorTexture.colorSpace = THREE.SRGBColorSpace
  visorTexture.needsUpdate = true

  // ── MODERN DESIGNER-TOY MATERIALS (CI ROYAL QUANTUM BLUE & SKY CYAN) ───
  const armorWhiteMat = new THREE.MeshPhysicalMaterial({
    color: 0x329dd2, // Rich Royal Blue (from user reference)
    roughness: 0.32,
    metalness: 0.1,
    clearcoat: 0.55,
    clearcoatRoughness: 0.15,
  })

  const silverAccentMat = new THREE.MeshStandardMaterial({
    color: 0x8fd5ec, // Light sky blue metallic
    roughness: 0.28,
    metalness: 0.35,
  })

  const darkTitaniumMat = new THREE.MeshStandardMaterial({
    color: 0x0f172a, // Deep slate/navy
    roughness: 0.38,
    metalness: 0.2,
  })

  const indigoMat = new THREE.MeshStandardMaterial({
    color: 0x0a3a4f, // Navy blue
    roughness: 0.38,
    metalness: 0.12,
  })

  const coralMat = new THREE.MeshStandardMaterial({
    color: 0x329dd2, // Sky Cyan inner ear (from user reference)
    roughness: 0.35,
    metalness: 0.0,
    polygonOffset: true,
    polygonOffsetFactor: -2,
    polygonOffsetUnits: -2,
  })

  const cyanGlowMat = new THREE.MeshBasicMaterial({
    color: 0x329dd2, // Electric Sky Cyan glow
  })

  const visorMat = new THREE.MeshBasicMaterial({
    map: visorTexture,
    transparent: true,
    alphaTest: 0.02,
  })

  // ── BUILD 4-LEGGED SEATED ROBOT CAT (NO BASE, NO SHADOW, NO FLOAT BALLS) ─
  const catRoot = new THREE.Group()
  catRoot.position.set(0, -0.44, 0)
  scene.add(catRoot)

  const bodyRig = new THREE.Group()
  catRoot.add(bodyRig)

  // 1. Sculpted Seated Feline Torso (Smooth organic chest-to-hip silhouette)
  const torsoPoints = [
    new THREE.Vector2(0.0, 0.02),
    new THREE.Vector2(0.18, 0.02),
    new THREE.Vector2(0.23, 0.08),
    new THREE.Vector2(0.245, 0.18),
    new THREE.Vector2(0.24, 0.30),
    new THREE.Vector2(0.225, 0.41),
    new THREE.Vector2(0.19, 0.50),
    new THREE.Vector2(0.15, 0.55),
    new THREE.Vector2(0.0, 0.56),
  ]
  const torsoGeo = new THREE.LatheGeometry(torsoPoints, 48)
  const torsoMesh = new THREE.Mesh(torsoGeo, armorWhiteMat)
  torsoMesh.scale.set(1.02, 1.0, 0.78)
  torsoMesh.position.set(0, 0, -0.01)
  bodyRig.add(torsoMesh)

  // Sleek Dark Titanium Collar Ring + Cyan Trim at Neck
  const collarRing = new THREE.Mesh(
    new THREE.TorusGeometry(0.195, 0.026, 24, 48),
    darkTitaniumMat
  )
  collarRing.rotation.x = Math.PI / 2 + 0.06
  collarRing.position.set(0, 0.52, 0.01)
  bodyRig.add(collarRing)

  const collarGlowTrim = new THREE.Mesh(
    new THREE.TorusGeometry(0.202, 0.005, 16, 48),
    cyanGlowMat
  )
  collarGlowTrim.rotation.x = Math.PI / 2 + 0.06
  collarGlowTrim.position.set(0, 0.502, 0.012)
  bodyRig.add(collarGlowTrim)

  // Chest SiamQuantum Logo Emblem (Mounted on upper sternum)
  const coreGroup = new THREE.Group()
  coreGroup.position.set(0, 0.34, 0.182)
  coreGroup.rotation.x = -0.13
  bodyRig.add(coreGroup)

  const coreHousing = new THREE.Mesh(
    new THREE.CylinderGeometry(0.088, 0.094, 0.020, 40),
    darkTitaniumMat
  )
  coreHousing.rotation.x = Math.PI / 2
  coreGroup.add(coreHousing)

  const coreGlowRing = new THREE.Mesh(
    new THREE.RingGeometry(0.076, 0.084, 40),
    cyanGlowMat
  )
  coreGlowRing.position.z = 0.011
  coreGroup.add(coreGlowRing)

  // High-res Canvas Texture for SiamQuantum Atlas Chest Logo
  const logoCanvas = document.createElement('canvas')
  logoCanvas.width = 512
  logoCanvas.height = 512
  const lctx = logoCanvas.getContext('2d')!

  function renderChestLogo(img?: HTMLImageElement) {
    const lw = logoCanvas.width
    const lh = logoCanvas.height
    lctx.clearRect(0, 0, lw, lh)

    // Deep OLED Dark Background
    lctx.fillStyle = '#080f1b'
    lctx.beginPath()
    lctx.arc(lw / 2, lh / 2, lw / 2, 0, Math.PI * 2)
    lctx.fill()

    // Subtle radial white core glow behind logo
    const grad = lctx.createRadialGradient(lw / 2, lh / 2, 10, lw / 2, lh / 2, lw * 0.45)
    grad.addColorStop(0, 'rgba(255, 255, 255, 0.16)')
    grad.addColorStop(1, 'rgba(255, 255, 255, 0.0)')
    lctx.fillStyle = grad
    lctx.beginPath()
    lctx.arc(lw / 2, lh / 2, lw / 2, 0, Math.PI * 2)
    lctx.fill()

    if (img && img.complete && img.naturalWidth > 0) {
      const targetW = lw * 0.76
      const aspect = img.naturalHeight / img.naturalWidth
      const targetH = targetW * aspect
      const dx = (lw - targetW) / 2
      const dy = (lh - targetH) / 2
      lctx.drawImage(img, dx, dy, targetW, targetH)
    }
  }

  renderChestLogo()
  const logoTexture = new THREE.CanvasTexture(logoCanvas)
  logoTexture.colorSpace = THREE.SRGBColorSpace
  logoTexture.anisotropy = 4

  const logoImg = new Image()
  logoImg.crossOrigin = 'anonymous'
  logoImg.onload = () => {
    renderChestLogo(logoImg)
    logoTexture.needsUpdate = true
  }
  logoImg.src = '/SQAlogo.png'

  const coreLogoDisc = new THREE.Mesh(
    new THREE.CircleGeometry(0.075, 40),
    new THREE.MeshBasicMaterial({ map: logoTexture })
  )
  coreLogoDisc.position.z = 0.0115
  coreGroup.add(coreLogoDisc)

  // Helper to build a seamless 3-toed sculpted porcelain cat paw (no grey dots!)
  function createSculptedPaw(scale = 1.0) {
    const pawGroup = new THREE.Group()

    // Center main toe pad
    const centerToe = new THREE.Mesh(
      new THREE.SphereGeometry(0.052 * scale, 24, 20),
      armorWhiteMat
    )
    centerToe.scale.set(0.92, 0.62, 1.28)
    centerToe.position.set(0, 0, 0.010 * scale)
    pawGroup.add(centerToe)

    // Left & Right outer toes merged cleanly on sides
    ;[-1, 1].forEach((ts) => {
      const sideToe = new THREE.Mesh(
        new THREE.SphereGeometry(0.044 * scale, 20, 18),
        armorWhiteMat
      )
      sideToe.scale.set(0.88, 0.58, 1.18)
      sideToe.rotation.y = ts * 0.16
      sideToe.position.set(ts * 0.032 * scale, -0.002 * scale, 0.002 * scale)
      pawGroup.add(sideToe)
    })

    return pawGroup
  }

  // ── 2. SCULPTED ELONGATED FRONT LEGS & SEATED PAWS ──────────────────────
  ;[-1, 1].forEach((side) => {
    const frontLegGroup = new THREE.Group()

    // 1. ปรับขาหน้าให้มี Slope สวยขึ้น และยื่นมาข้างหน้าเพื่อสร้างมิติ
    const legCurve = new THREE.CatmullRomCurve3([
      new THREE.Vector3(side * 0.135, 0.42, 0.03),  // กางช่วงไหล่ออกอีกนิด
      new THREE.Vector3(side * 0.128, 0.28, 0.12),
      new THREE.Vector3(side * 0.112, 0.15, 0.18),  // ดันไปข้างหน้า
      new THREE.Vector3(side * 0.095, 0.035, 0.21), // หุบเข้าหากันเล็กน้อยตรงปลาย และยื่นไปข้างหน้า
    ])

    // ลดรัศมีท่อลงเล็กน้อย (0.046 -> 0.038) เพื่อให้แขนดูเรียวยาว สมส่วน ไม่บวมตัน
    const legGeo = new THREE.TubeGeometry(legCurve, 36, 0.038, 24, false)
    const legMesh = new THREE.Mesh(legGeo, armorWhiteMat)
    frontLegGroup.add(legMesh)

    // ไหล่ทรงมน (Shoulder Cap) เพื่อให้จุดต่อระหว่างต้นแขนกับตัวดูแนบเนียน ไม่เห็นปลายท่อตัดตรง
    const shoulderCap = new THREE.Mesh(
      new THREE.SphereGeometry(0.042, 20, 16),
      armorWhiteMat
    )
    shoulderCap.position.set(side * 0.135, 0.42, 0.03)
    frontLegGroup.add(shoulderCap)

    // 2. วงแหวนเรืองแสงที่ข้อเท้า ปรับตำแหน่งให้อยู่เหนืออุ้งเท้าพอดี
    const wristGlow = new THREE.Mesh(
      new THREE.TorusGeometry(0.0385, 0.0035, 14, 32),
      cyanGlowMat
    )
    wristGlow.rotation.x = Math.PI / 2 + 0.08
    wristGlow.position.set(side * 0.095, 0.048, 0.212)
    frontLegGroup.add(wristGlow)

    // 3. อุ้งเท้าหน้า วางระนาบพอดีกับพื้น (y: 0.022)
    const frontPaw = createSculptedPaw(0.95)
    frontPaw.position.set(side * 0.095, 0.022, 0.225)
    frontLegGroup.add(frontPaw)

    bodyRig.add(frontLegGroup)
  })

  // 3. Seated Hind Haunches & Tucked Rear Paws (Folded cleanly on left & right sides)
  ;[-1, 1].forEach((side) => {
    const haunchGroup = new THREE.Group()
    // 2. ปรับขาหลัง (Hind Haunches) ให้กลืนเข้าลำตัวและถอยไปข้างหลัง
    haunchGroup.position.set(side * 0.195, 0.145, -0.04)

    // Smooth Folded Thigh Haunch
    const haunchMesh = new THREE.Mesh(
      new THREE.SphereGeometry(0.145, 28, 24),
      armorWhiteMat
    )
    haunchMesh.scale.set(0.66, 1.02, 1.14)
    haunchMesh.rotation.z = side * 0.14
    haunchMesh.rotation.y = side * 0.08
    haunchGroup.add(haunchMesh)

    // Flush Mechanical Hip Joint Cap + Concentric Cyan Ring
    const hipCap = new THREE.Mesh(
      new THREE.CylinderGeometry(0.058, 0.062, 0.022, 28),
      darkTitaniumMat
    )
    hipCap.rotation.z = Math.PI / 2 - side * 0.12
    hipCap.position.set(side * 0.092, -0.005, 0.022)
    haunchGroup.add(hipCap)

    const hipGlowRing = new THREE.Mesh(
      new THREE.TorusGeometry(0.045, 0.0055, 14, 28),
      cyanGlowMat
    )
    hipGlowRing.rotation.y = Math.PI / 2
    hipGlowRing.rotation.x = side * 0.12
    hipGlowRing.position.set(side * 0.104, -0.005, 0.022)
    haunchGroup.add(hipGlowRing)

    // 3-Toed Sculpted Rear Paw tucked forward on the exact same floor plane
    const rearPaw = createSculptedPaw(0.92)
    rearPaw.rotation.y = side * -0.18
    rearPaw.position.set(side * 0.024, -0.120, 0.128)
    haunchGroup.add(rearPaw)

    bodyRig.add(haunchGroup)
  })

  // ── 4. SCULPTED ROBOT CAT HEAD & CURVED OLED VISOR ──────────────────────
  const headRig = new THREE.Group()
  headRig.position.set(0, 0.84, 0.02)
  bodyRig.add(headRig)

  const helmetMesh = new THREE.Mesh(
    new THREE.SphereGeometry(0.35, 48, 36),
    armorWhiteMat
  )
  helmetMesh.scale.set(1.14, 0.94, 0.98)
  headRig.add(helmetMesh)

  const visorScreen = new THREE.Mesh(
    new THREE.SphereGeometry(
      0.353,
      64,
      48,
      Math.PI * 0.18,
      Math.PI * 0.64,
      Math.PI * 0.23,
      Math.PI * 0.52
    ),
    visorMat
  )
  visorScreen.scale.set(1.14, 0.94, 0.99)
  headRig.add(visorScreen)

  // ── 5. ROUNDED DESIGNER CAT EARS ────────────────────────────────────────
  const earShape = new THREE.Shape()
  earShape.moveTo(-0.095, -0.02)
  earShape.quadraticCurveTo(-0.115, 0.13, -0.02, 0.22)
  earShape.quadraticCurveTo(0, 0.235, 0.02, 0.22)
  earShape.quadraticCurveTo(0.115, 0.13, 0.095, -0.02)
  earShape.quadraticCurveTo(0, -0.06, -0.095, -0.02)
  earShape.closePath()

  const earGeo = new THREE.ExtrudeGeometry(earShape, {
    depth: 0.026,
    bevelEnabled: true,
    bevelSegments: 8,
    steps: 1,
    bevelSize: 0.026,
    bevelThickness: 0.032,
  })
  earGeo.center()

  const innerEarShape = new THREE.Shape()
  innerEarShape.moveTo(-0.058, -0.02)
  innerEarShape.quadraticCurveTo(-0.065, 0.07, -0.01, 0.125)
  innerEarShape.quadraticCurveTo(0, 0.135, 0.01, 0.125)
  innerEarShape.quadraticCurveTo(0.065, 0.07, 0.058, -0.02)
  innerEarShape.quadraticCurveTo(0, -0.04, -0.058, -0.02)
  innerEarShape.closePath()

  const innerEarGeo = new THREE.ShapeGeometry(innerEarShape)

  const ears: any[] = []
  ;[-1, 1].forEach((side) => {
    const earGroup = new THREE.Group()
    earGroup.position.set(side * 0.205, 0.245, -0.01)
    earGroup.rotation.z = side * -0.27
    earGroup.rotation.y = side * 0.15
    earGroup.rotation.x = 0.06

    const outerEar = new THREE.Mesh(earGeo, armorWhiteMat)
    earGroup.add(outerEar)

    const innerEar = new THREE.Mesh(innerEarGeo, coralMat)
    innerEar.position.set(0, -0.015, 0.047)
    earGroup.add(innerEar)

    headRig.add(earGroup)
    ears.push(earGroup)
  })

  // ── 6. SLEEK ACOUSTIC EAR PODS ──────────────────────────────────────────
  ;[-1, 1].forEach((side) => {
    const podGroup = new THREE.Group()
    podGroup.position.set(side * 0.385, 0.0, -0.01)
    podGroup.rotation.z = Math.PI / 2

    const podCap = new THREE.Mesh(
      new THREE.CylinderGeometry(0.082, 0.090, 0.03, 32),
      darkTitaniumMat
    )
    podGroup.add(podCap)

    const podRing = new THREE.Mesh(
      new THREE.TorusGeometry(0.064, 0.008, 12, 32),
      cyanGlowMat
    )
    podRing.rotation.x = Math.PI / 2
    podRing.position.y = side * -0.016
    podGroup.add(podRing)

    headRig.add(podGroup)
  })

  // ── 7. SMOOTH CURVED ROBOTIC TAIL ───────────────────────────────────────
  const tailGroup = new THREE.Group()
  tailGroup.position.set(0, 0.09, -0.15)
  bodyRig.add(tailGroup)

  const tailCurve = new THREE.CatmullRomCurve3([
    new THREE.Vector3(0, 0, 0),
    new THREE.Vector3(0.20, 0.02, -0.04),
    new THREE.Vector3(0.37, 0.14, 0.01),
    new THREE.Vector3(0.44, 0.31, 0.05),
    new THREE.Vector3(0.39, 0.45, 0.07),
  ])

  const tailRadius = 0.038
  const tailGeo = new THREE.TubeGeometry(tailCurve, 44, tailRadius, 24, false)
  const tailMesh = new THREE.Mesh(tailGeo, armorWhiteMat)
  tailGroup.add(tailMesh)

  // Concentric Cyan Energy Bands aligned strictly perpendicular to the tail curve
  ;[0.62, 0.80].forEach((u) => {
    const pt = tailCurve.getPointAt(u)
    const tangent = tailCurve.getTangentAt(u).normalize()
    const band = new THREE.Mesh(
      new THREE.TorusGeometry(tailRadius + 0.002, 0.0055, 16, 32),
      cyanGlowMat
    )
    band.position.copy(pt)
    band.quaternion.setFromUnitVectors(new THREE.Vector3(0, 0, 1), tangent)
    tailGroup.add(band)
  })

  const tailTipCap = new THREE.Mesh(
    new THREE.SphereGeometry(tailRadius, 24, 24),
    armorWhiteMat
  )
  tailTipCap.position.copy(tailCurve.getPointAt(1.0))
  tailGroup.add(tailTipCap)

  // ── POINTER LOOK TRACKING + IDLE ANIMATION LOOP ─────────────────────────
  const pointer = { x: 0, y: 0, active: false }
  const smoothLook = { x: 0, y: 0 }

  function handleGlobalPointerMove(e: PointerEvent) {
    if (!wrap) return
    const rc = wrap.getBoundingClientRect()
    const catCenterX = rc.left + rc.width / 2
    const catCenterY = rc.top + rc.height * 0.44

    const dx = (e.clientX - catCenterX) / (window.innerWidth * 0.42)
    const dy = (e.clientY - catCenterY) / (window.innerHeight * 0.42)

    pointer.x = Math.max(-1.15, Math.min(1.15, dx))
    pointer.y = Math.max(-1.15, Math.min(1.15, dy))
    pointer.active = true
  }

  window.addEventListener('pointermove', handleGlobalPointerMove)

  let t = 0
  let blinkState = 0
  let nextBlinkTime = 2.8

  function animate() {
    animId = requestAnimationFrame(animate)
    t += 0.018

    // 1. Subtle breathing motion
    const breath = Math.sin(t * 2.0) * 0.008
    bodyRig.position.y = breath

    // 2. Smooth Pointer Look Tracking
    const targetX = pointer.active ? pointer.x : Math.sin(t * 0.55) * 0.22
    const targetY = pointer.active ? pointer.y : Math.cos(t * 0.45) * 0.10

    smoothLook.x += (targetX - smoothLook.x) * 0.10
    smoothLook.y += (targetY - smoothLook.y) * 0.10

    headRig.rotation.y = smoothLook.x * 0.48
    headRig.rotation.x = smoothLook.y * 0.28
    headRig.rotation.z = -smoothLook.x * 0.07

    bodyRig.rotation.y = smoothLook.x * 0.12

    // 3. Cute ear micro-twitch
    const twitchWindow = t % 4.5
    const earTwitch = twitchWindow > 4.1 ? Math.sin(t * 28.0) * 0.07 : 0
    ears[0].rotation.z = -0.27 + earTwitch - smoothLook.x * 0.04
    ears[1].rotation.z = 0.27 - earTwitch * 0.6 - smoothLook.x * 0.04

    // 4. OLED Face Blinking & Gaze Update
    if (t > nextBlinkTime) {
      blinkState = Math.sin(((t - nextBlinkTime) / 0.22) * Math.PI)
      if (t - nextBlinkTime >= 0.22) {
        blinkState = 0
        nextBlinkTime = t + 2.8 + Math.random() * 2.5
      }
    }
    drawVisorFace(
      Math.max(0, Math.min(0.95, blinkState)),
      t * 2.5,
      smoothLook.x,
      smoothLook.y
    )
    visorTexture.needsUpdate = true

    // 5. Tail gentle sway
    tailGroup.rotation.y = Math.sin(t * 1.5) * 0.18
    tailGroup.rotation.z = Math.cos(t * 1.2) * 0.06

    renderer.render(scene, camera)
  }
  animate()

  const handleResize = () => {
    if (!wrap || !renderer || !camera || resizeFrame !== null) return
    resizeFrame = requestAnimationFrame(() => {
      resizeFrame = null
      if (!wrap || !renderer || !camera) return
      const nW = wrap.clientWidth
      const nH = wrap.clientHeight || 440
      if (nW === 0 || nH === 0) return
      camera.aspect = nW / nH
      camera.position.z = baseCameraZ * Math.max(1, baseAspect / camera.aspect)
      camera.updateProjectionMatrix()
      renderer.setSize(nW, nH, false)
    })
  }

  window.addEventListener('resize', handleResize)
  resizeObserver = new ResizeObserver(handleResize)
  resizeObserver.observe(wrap)

  onUnmounted(() => {
    window.removeEventListener('pointermove', handleGlobalPointerMove)
    window.removeEventListener('resize', handleResize)
    resizeObserver?.disconnect()
    if (resizeFrame !== null) cancelAnimationFrame(resizeFrame)
  })
}

onMounted(() => {
  initRobotCat()
})

onUnmounted(() => {
  if (animId !== null) cancelAnimationFrame(animId)
  if (renderer && renderer.domElement) {
    renderer.dispose()
    renderer.domElement.remove()
  }
})
</script>

<template>
  <div
    ref="containerRef"
    class="w-full h-full flex items-center justify-center relative select-none"
    :style="{ height: typeof height === 'number' ? `${height}px` : height }"
  />
</template>
