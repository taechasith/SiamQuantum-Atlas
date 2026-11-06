<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import type { GraphNode, GraphLink } from '@/types'

const props = defineProps<{
  nodes: GraphNode[]
  links: GraphLink[]
}>()

const emit = defineEmits<{
  (e: 'node-click', node: GraphNode): void
  (e: 'link-click', link: GraphLink): void
}>()

const containerRef = ref<HTMLDivElement | null>(null)
let graphInstance: any = null
let resizeObserver: ResizeObserver | null = null

function getClonedData() {
  return {
    nodes: (props.nodes || []).map((n) => ({ ...n })),
    links: (props.links || []).map((l) => ({
      ...l,
      source: typeof l.source === 'object' ? l.source.id : l.source,
      target: typeof l.target === 'object' ? l.target.id : l.target,
    })),
  }
}

async function initGraph() {
  if (!containerRef.value) return

  const ForceGraph: any = (await import('force-graph')).default

  if (graphInstance) {
    graphInstance._destructor?.()
  }

  const width = containerRef.value.clientWidth || window.innerWidth
  const height = containerRef.value.clientHeight || window.innerHeight

  graphInstance = ForceGraph()(containerRef.value)
    .backgroundColor('#000000')
    .width(width)
    .height(height)
    .nodeId('id')
    .nodeLabel('label')
    .nodeVal('val')
    .nodeRelSize(3.2)
    .nodeColor((node: GraphNode) => {
      const value = Number(node.val ?? 1)
      if (value >= 20) return '#e0f2fe'
      if (value >= 8) return '#93c5fd'
      if (value >= 3) return '#60a5fa'
      return '#3b82f6'
    })
    .linkSource('source')
    .linkTarget('target')
    .linkLabel('label')
    .linkColor(() => 'rgba(96, 165, 250, 0.24)')
    .linkWidth(0.9)
    .cooldownTicks(140)
    .d3VelocityDecay(0.32)
    .onNodeClick((node: GraphNode) => {
      emit('node-click', node)
    })
    .onLinkClick((link: GraphLink) => {
      emit('link-click', link)
    })
    .onEngineStop(() => {
      graphInstance?.zoomToFit(600, 72)
    })
    .graphData(getClonedData())

  resizeObserver = new ResizeObserver((entries) => {
    for (const entry of entries) {
      const { width: w, height: h } = entry.contentRect
      if (w > 0 && h > 0 && graphInstance) {
        graphInstance.width(w)
        graphInstance.height(h)
      }
    }
  })
  resizeObserver.observe(containerRef.value)
}

watch(
  () => [props.nodes?.length, props.links?.length],
  () => {
    if (graphInstance && props.nodes && props.links) {
      graphInstance.graphData(getClonedData())
    }
  }
)

onMounted(() => {
  initGraph()
})

onUnmounted(() => {
  if (resizeObserver) {
    resizeObserver.disconnect()
  }
  if (graphInstance && graphInstance._destructor) {
    graphInstance._destructor()
  }
})
</script>

<template>
  <div ref="containerRef" class="relative h-full w-full overflow-hidden bg-black" />
</template>
