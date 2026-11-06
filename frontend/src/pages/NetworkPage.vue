<script setup lang="ts">
import { ref, computed } from 'vue'
import { useNetworkGraphQuery, type NetworkData, type NetworkNode, type NetworkLink } from '@/api/queries'
import NetworkGraph from '@/components/NetworkGraph.vue'
import Badge from '@/components/ui/Badge.vue'
import Alert from '@/components/ui/Alert.vue'
import { t } from '@/composables/useLocale'

const { data: graphData, isPending, isError, error, refetch } = useNetworkGraphQuery()

const selectedDomain = ref('all')
const selectedNode = ref<any | null>(null)
const showAllNodes = ref(false)

const domains = [
  { id: 'all', label: 'All Domains' },
  { id: 'qkd', label: 'QKD & Cryptography' },
  { id: 'computing', label: 'Quantum Computing' },
  { id: 'sensing', label: 'Quantum Sensing' },
  { id: 'policy', label: 'National Policy' },
]

const baseGraphData = computed<NetworkData>(() => {
  if (!graphData.value) {
    return { nodes: [], links: [] }
  }

  if (selectedDomain.value === 'all') {
    return graphData.value
  }

  const filteredNodes = graphData.value.nodes.filter(
    (n: NetworkNode) => n.group?.toLowerCase() === selectedDomain.value || n.category?.toLowerCase() === selectedDomain.value
  )
  const nodeIds = new Set(filteredNodes.map((n: NetworkNode) => n.id))

  const filteredLinks = graphData.value.links.filter(
    (l: NetworkLink) => nodeIds.has(typeof l.source === 'object' ? l.source.id : l.source) &&
           nodeIds.has(typeof l.target === 'object' ? l.target.id : l.target)
  )

  return {
    nodes: filteredNodes,
    links: filteredLinks,
  }
})

const filteredGraphData = computed<NetworkData>(() => {
  const base = baseGraphData.value
  if (showAllNodes.value) return base

  const degree = new Map<string, number>()
  for (const node of base.nodes) degree.set(node.id, 0)
  for (const link of base.links) {
    const source = typeof link.source === 'object' ? link.source.id : link.source
    const target = typeof link.target === 'object' ? link.target.id : link.target
    degree.set(source, (degree.get(source) ?? 0) + 1)
    degree.set(target, (degree.get(target) ?? 0) + 1)
  }

  const coreNodes = base.nodes.filter((node) => (degree.get(node.id) ?? 0) >= 2)
  const coreIds = new Set(coreNodes.map((node) => node.id))
  const adjacency = new Map<string, Set<string>>()
  for (const node of coreNodes) adjacency.set(node.id, new Set())

  for (const link of base.links) {
    const source = typeof link.source === 'object' ? link.source.id : link.source
    const target = typeof link.target === 'object' ? link.target.id : link.target
    if (!coreIds.has(source) || !coreIds.has(target)) continue
    adjacency.get(source)?.add(target)
    adjacency.get(target)?.add(source)
  }

  const visited = new Set<string>()
  let largestComponent = new Set<string>()

  for (const node of coreNodes) {
    if (visited.has(node.id)) continue
    const component = new Set<string>()
    const stack = [node.id]
    visited.add(node.id)
    while (stack.length) {
      const current = stack.pop()
      if (!current) continue
      component.add(current)
      for (const neighbor of adjacency.get(current) ?? []) {
        if (visited.has(neighbor)) continue
        visited.add(neighbor)
        stack.push(neighbor)
      }
    }
    if (component.size > largestComponent.size) largestComponent = component
  }

  const nodes = coreNodes.filter((node) => largestComponent.has(node.id))
  const links = base.links.filter((link) => {
    const source = typeof link.source === 'object' ? link.source.id : link.source
    const target = typeof link.target === 'object' ? link.target.id : link.target
    return largestComponent.has(source) && largestComponent.has(target)
  })

  return { nodes, links }
})

function onNodeClick(node: any) {
  selectedNode.value = node
}
</script>

<template>
  <div class="h-full w-full relative overflow-hidden bg-black">
    <!-- 1. isPending -->
    <div v-if="isPending" class="h-full w-full flex flex-col items-center justify-center gap-4 bg-black">
      <div class="w-10 h-10 rounded-full bg-sky-400/20 animate-ping" />
      <div class="text-center space-y-1">
        <p class="text-xs font-mono text-slate-300">{{ t('Initializing 2D Force-Directed Simulation...') }}</p>
        <p class="text-[11px] text-slate-500">{{ t('Computing 2,090+ triplet edges & clustering') }}</p>
      </div>
    </div>

    <!-- 2. isError -->
    <div v-else-if="isError" class="h-full w-full flex flex-col items-center justify-center p-8 bg-black">
      <Alert variant="danger" :title="t('Network Graph Engine Error')">
        {{ error?.message || t('Failed to initialize force layout.') }}
      </Alert>
      <button
        @click="() => refetch()"
        class="mt-4 px-4 py-2 rounded-xl bg-sky-400 text-slate-950 font-bold text-xs cursor-pointer"
      >
        {{ t('Retry Graph Engine') }}
      </button>
    </div>

    <!-- 3. Active Fullscreen 2D Canvas (Zero Card, Zero Padding, Zero Gap) -->
    <div v-else class="h-full w-full relative overflow-hidden">
      <!-- Fullscreen 2D Graph Canvas -->
      <NetworkGraph
        :nodes="filteredGraphData.nodes"
        :links="filteredGraphData.links"
        class="w-full h-full"
        @node-click="onNodeClick"
      />

      <!-- Node Inspection Side Sheet (Floating Royal Blue Glass Card, Only on Node Click) -->
      <transition
        enter-active-class="transition duration-300 ease-out"
        enter-from-class="opacity-0 translate-y-8"
        enter-to-class="opacity-100 translate-y-0"
        leave-active-class="transition duration-200 ease-in"
        leave-from-class="opacity-100 translate-y-0"
        leave-to-class="opacity-0 translate-y-8"
      >
        <div
          v-if="selectedNode"
          class="absolute bottom-4 left-4 right-4 z-20 flex max-h-[60%] flex-col justify-between rounded-[26px] p-5 text-white md:bottom-5 md:left-auto md:right-5 md:max-h-[calc(100%-6rem)] md:w-96"
          style="background: linear-gradient(180deg, #1d4ed8 0%, rgba(29, 78, 216, 0.6) 60%, rgba(29, 78, 216, 0.2) 100%); backdrop-filter: blur(14px);"
        >
          <div class="min-h-0 space-y-3.5 overflow-y-auto pr-1">
            <div class="flex items-start justify-between">
              <span class="px-2.5 py-0.5 rounded-full text-xs font-mono font-bold bg-black/55 text-white">
                {{ selectedNode.group || t('Concept Entity') }}
              </span>
              <button
                @click="selectedNode = null"
                class="w-7 h-7 rounded-full bg-white/10 hover:bg-white/20 text-white flex items-center justify-center text-xs transition cursor-pointer"
              >
                ✕
              </button>
            </div>

            <div>
              <h3 class="text-lg font-bold text-white tracking-tight leading-snug">
                {{ selectedNode.label || selectedNode.id }}
              </h3>
              <p class="text-[11px] font-mono text-sky-200/70 mt-0.5">{{ t('Entity ID:') }} {{ selectedNode.id }}</p>
            </div>

            <div class="grid grid-cols-2 gap-2 text-xs font-mono py-2.5 bg-black/30 p-2.5 rounded-xl">
              <div class="space-y-0.5">
                <span class="text-[10px] text-white/60 uppercase">{{ t('Centrality') }}</span>
                <span class="text-sky-300 font-bold text-sm block">{{ selectedNode.val || 1 }}</span>
              </div>
              <div v-if="selectedNode.group" class="space-y-0.5">
                <span class="text-[10px] text-white/60 uppercase">{{ t('Domain') }}</span>
                <span class="text-emerald-300 font-bold text-sm block">{{ t(selectedNode.group) }}</span>
              </div>
            </div>

            <div class="space-y-1.5">
              <span class="text-[10px] font-mono text-white/60 uppercase font-semibold">{{ t('Discourse Context') }}</span>
              <p class="text-xs text-white/85 leading-relaxed bg-black/25 p-2.5 rounded-xl">
                {{ t('This concept is extracted from subject–relation–object triplets in the current corpus. Select Browse Related Sources to inspect the evidence.') }}
              </p>
            </div>
          </div>

          <div class="pt-3">
            <router-link
              :to="{ path: '/database', query: { q: selectedNode.label || selectedNode.id } }"
              class="w-full py-2.5 rounded-xl bg-white hover:bg-slate-100 text-slate-950 font-bold text-xs text-center transition flex items-center justify-center gap-1.5 cursor-pointer"
            >
              {{ t('Browse Related Sources ↗') }}
            </router-link>
          </div>
        </div>
      </transition>

      <button
        v-if="!selectedNode"
        type="button"
        class="absolute bottom-5 left-5 z-20 rounded-full border border-sky-300/20 bg-[#051028]/85 px-3 py-2 font-mono text-[10px] text-white/70 shadow-lg transition hover:border-sky-300/40 hover:text-white"
        @click="showAllNodes = !showAllNodes"
      >
        {{ showAllNodes ? t('Core network only') : t('Show all nodes') }}
      </button>
    </div>
  </div>
</template>
