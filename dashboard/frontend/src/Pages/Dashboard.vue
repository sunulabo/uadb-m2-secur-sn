<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { router } from '@inertiajs/vue3'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'

const props = defineProps({ operational: { type: Array, default: () => [] }, analytical_24h: { type: Array, default: () => [] }, fallback: { type: Array, default: () => [] }, errors: { type: Array, default: () => [] } })
const selected = ref(null), mapElement = ref(null)
let map, layer, timer
const rows = computed(() => [...props.operational, ...props.analytical_24h, ...props.fallback].reduce((out, row) => { if (!out.some(item => item.id === row.id)) out.push(row); return out }, []).sort((a, b) => Number(b.score_risque || 0) - Number(a.score_risque || 0)))
const redCount = computed(() => rows.value.filter(row => row.niveau_risque === 'ROUGE').length)
const maxScore = computed(() => Math.max(...rows.value.map(row => Number(row.score_risque) || 0), 0).toFixed(1))
const color = level => ({ VERT: '#2f8f62', ORANGE: '#c98322', ROUGE: '#c54b47' }[level] || '#71808c')
function drawMap() {
  if (!map) { map = L.map(mapElement.value, { zoomControl: true, attributionControl: false }).setView([14.7, -16.8], 7); layer = L.layerGroup().addTo(map) }
  layer.clearLayers()
  rows.value.filter(row => row.latitude && row.longitude).forEach(row => { const point = L.circleMarker([row.latitude, row.longitude], { radius: Math.max(7, Math.min(22, 7 + Number(row.score_risque || 0) / 3)), color: '#fff', weight: 2, fillColor: color(row.niveau_risque), fillOpacity: .88 }); point.bindTooltip(`${row.zone} · ${Number(row.score_risque || 0).toFixed(1)}`); point.on('click', () => { selected.value = row }).addTo(layer) })
}
onMounted(() => { drawMap(); timer = setInterval(() => router.reload({ only: ['operational', 'analytical_24h', 'errors'], preserveScroll: true, preserveState: true, onSuccess: drawMap }), 30000) })
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <header class="flex min-h-24 items-center justify-between border-b-4 border-[#e4b44b] bg-ink px-[4vw] py-5 text-white">
    <div><span class="text-[10px] font-bold tracking-[.16em] text-[#e4b44b]">SÉCURITÉ ROUTIÈRE · SÉNÉGAL</span><h1 class="mt-1 font-display text-3xl font-semibold">Centre de situation</h1></div>
    <span class="text-xs text-slate-300"><i class="mr-2 inline-block h-2 w-2 rounded-full bg-[#63bd83]" />Actualisé automatiquement</span>
  </header>
  <main class="mx-auto mb-14 mt-7 w-[92vw] max-w-[1500px]">
    <section class="mb-4 grid grid-cols-2 gap-3 lg:grid-cols-4">
      <div v-for="card in [{ label: 'Cellules opérationnelles', value: operational.length }, { label: 'Cellules 24 h', value: analytical_24h.length }, { label: 'Alertes rouges', value: redCount }, { label: 'Score maximal', value: maxScore }]" :key="card.label" class="border border-line bg-white px-5 py-4"><span class="block text-xs text-muted">{{ card.label }}</span><strong class="mt-2 block font-display text-3xl font-semibold">{{ card.value }}</strong></div>
    </section>
    <p v-if="errors.length" class="mb-4 border border-[#e9c98f] bg-[#fff3df] px-4 py-2 text-xs text-[#805816]">Mode dégradé · {{ errors.join(' · ') }}</p>
    <section class="grid gap-4 lg:grid-cols-[minmax(0,1.65fr)_minmax(300px,.65fr)]">
      <article class="border border-line bg-white"><div class="flex items-start justify-between border-b border-line px-5 py-4"><div><span class="text-[10px] font-bold tracking-[.16em] text-[#e4b44b]">GÉOGRAPHIE DU RISQUE</span><h2 class="mt-1 text-lg font-semibold">Cellules actives 2 km × 2 km</h2></div><div class="flex gap-3 text-[11px] text-muted"><span><i class="mr-1 inline-block h-2 w-2 rounded-full bg-signal-green" />Vert</span><span><i class="mr-1 inline-block h-2 w-2 rounded-full bg-signal-orange" />Orange</span><span><i class="mr-1 inline-block h-2 w-2 rounded-full bg-signal-red" />Rouge</span></div></div><div ref="mapElement" class="h-[480px] bg-[#dce9e5] lg:h-[610px]" /><footer class="px-4 py-2 text-[11px] text-muted">HBase · opérationnel court &nbsp;|&nbsp; Hive · agrégat 24 h</footer></article>
      <aside class="min-h-0 border border-line bg-white lg:min-h-[740px]"><div class="border-b border-line px-5 py-4"><span class="text-[10px] font-bold tracking-[.16em] text-[#e4b44b]">CELLULE SÉLECTIONNÉE</span><h2 class="mt-1 text-lg font-semibold">{{ selected?.zone || 'Aucune sélection' }}</h2></div><div v-if="selected" class="min-h-40 p-5"><strong class="block font-display text-3xl font-semibold">{{ Number(selected.score_risque || 0).toFixed(1) }}</strong><span class="my-2 inline-block px-2 py-1 text-[11px] font-bold text-white" :style="{ background: color(selected.niveau_risque) }">{{ selected.niveau_risque }}</span><dl class="grid grid-cols-2 gap-3 text-xs"><div><dt class="text-[11px] text-muted">Origine</dt><dd class="font-semibold">{{ selected.source }}</dd></div><div><dt class="text-[11px] text-muted">Cellule</dt><dd class="font-semibold">{{ selected.grid_2km_id || 'non renseignée' }}</dd></div><div><dt class="text-[11px] text-muted">Incidents</dt><dd class="font-semibold">{{ selected.nb_incidents }}</dd></div><div><dt class="text-[11px] text-muted">Victimes</dt><dd class="font-semibold">{{ selected.nb_victimes }}</dd></div></dl></div><div v-else class="min-h-40 p-5 text-sm leading-6 text-muted">Sélectionnez un point sur la carte pour afficher son niveau de risque.</div><div class="mt-3 border-y border-line px-5 py-4"><span class="text-[10px] font-bold tracking-[.16em] text-[#e4b44b]">PRIORITÉS</span><h2 class="mt-1 text-lg font-semibold">Hotspots à surveiller</h2></div><button v-for="row in rows.slice(0, 8)" :key="row.id" class="flex w-full cursor-pointer justify-between border-b border-line bg-white px-5 py-3 text-left text-ink hover:bg-[#f3f7f5]" @click="selected = row"><span><b class="block">{{ row.zone }}</b><small class="mt-1 block text-xs text-muted">{{ Number(row.score_risque || 0).toFixed(1) }} · {{ row.source }}</small></span><em class="text-[10px] font-bold not-italic" :class="{ 'text-signal-red': row.niveau_risque === 'ROUGE', 'text-signal-orange': row.niveau_risque === 'ORANGE', 'text-signal-green': row.niveau_risque === 'VERT' }">{{ row.niveau_risque }}</em></button></aside>
    </section>
  </main>
</template>
