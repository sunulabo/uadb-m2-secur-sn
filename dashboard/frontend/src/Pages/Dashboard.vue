<script setup>
import { computed, onMounted, onUnmounted, ref } from "vue";
import { router } from "@inertiajs/vue3";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

const props = defineProps({
  operational: { type: Array, default: () => [] },
  analytical_24h: { type: Array, default: () => [] },
  fallback: { type: Array, default: () => [] },
  errors: { type: Array, default: () => [] },
});
const activeView = ref("overview"),
  selected = ref(null),
  riskFilter = ref("ALL"),
  zoneFilter = ref("ALL"),
  mapElement = ref(null),
  refreshing = ref(false);
let map, layer, timer;
const rows = computed(() =>
  [...props.operational, ...props.analytical_24h, ...props.fallback]
    .reduce((out, row) => {
      if (!out.some((item) => item.id === row.id)) out.push(row);
      return out;
    }, [])
    .sort((a, b) => Number(b.score_risque || 0) - Number(a.score_risque || 0)),
);
const filteredRows = computed(() =>
  rows.value.filter(
    (row) =>
      (riskFilter.value === "ALL" || row.niveau_risque === riskFilter.value) &&
      (zoneFilter.value === "ALL" || row.zone === zoneFilter.value),
  ),
);
const zones = computed(() => [
  ...new Set(rows.value.map((row) => row.zone).filter(Boolean)),
]);
const redCount = computed(
  () => rows.value.filter((row) => row.niveau_risque === "ROUGE").length,
);
const maxScore = computed(() =>
  Math.max(
    ...rows.value.map((row) => Number(row.score_risque) || 0),
    0,
  ).toFixed(1),
);
const avgScore = computed(() =>
  (
    rows.value.reduce((sum, row) => sum + Number(row.score_risque || 0), 0) /
    Math.max(rows.value.length, 1)
  ).toFixed(1),
);
const nav = [
  { id: "overview", label: "Vue générale", icon: "◈" },
  { id: "map", label: "Carte opérationnelle", icon: "⌖" },
  { id: "analytics", label: "Analytique 24 h", icon: "◒" },
  { id: "alerts", label: "Alertes HBase", icon: "!" },
  { id: "ml", label: "ML & recommandations", icon: "↗" },
  { id: "quality", label: "Qualité des données", icon: "✓" },
];
const color = (level) =>
  ({ VERT: "#2f8f62", ORANGE: "#c98322", ROUGE: "#c54b47" })[level] ||
  "#71808c";
const label = (level) =>
  ({ VERT: "Faible", ORANGE: "Modéré", ROUGE: "Critique" })[level] || level;
const refresh = () => {
  refreshing.value = true;
  router.reload({
    only: ["operational", "analytical_24h", "errors"],
    preserveScroll: true,
    preserveState: true,
    onSuccess: () => {
      drawMap();
      refreshing.value = false;
    },
    onError: () => {
      refreshing.value = false;
    },
  });
};
function drawMap() {
  if (!mapElement.value) return;
  if (!map) {
    map = L.map(mapElement.value, {
      zoomControl: true,
      attributionControl: false,
    }).setView([14.7, -16.8], 7);
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 18,
    }).addTo(map);
    layer = L.layerGroup().addTo(map);
  }
  layer.clearLayers();
  filteredRows.value
    .filter((row) => row.latitude && row.longitude)
    .forEach((row) => {
      const point = L.circleMarker([row.latitude, row.longitude], {
        radius: Math.max(
          7,
          Math.min(22, 7 + Number(row.score_risque || 0) / 3),
        ),
        color: "#fff",
        weight: 2,
        fillColor: color(row.niveau_risque),
        fillOpacity: 0.9,
      });
      point.bindTooltip(
        `${row.zone} · ${Number(row.score_risque || 0).toFixed(1)}`,
      );
      point.on("click", () => {
        selected.value = row;
      });
      point.addTo(layer);
    });
}
function selectView(view) {
  activeView.value = view;
  if (view === "map") setTimeout(drawMap, 80);
}
function barWidth(row) {
  return `${Math.min(100, Math.max(8, Number(row.score_risque || 0) * 2))}%`;
}
onMounted(() => {
  timer = setInterval(refresh, 30000);
});
onUnmounted(() => {
  clearInterval(timer);
  if (map) map.remove();
});
</script>

<template>
  <div class="min-h-screen bg-paper text-ink">
    <aside
      class="fixed inset-y-0 left-0 z-20 hidden w-64 flex-col bg-ink text-white lg:flex"
    >
      <div class="border-b border-white/10 px-7 py-7">
        <p class="text-[10px] font-bold tracking-[.2em] text-signal-yellow">
          SECUR-SN
        </p>
        <h1 class="mt-3 font-display text-2xl">Centre de situation</h1>
        <p class="mt-1 text-xs text-slate-400">
          Intelligence routière · Sénégal
        </p>
      </div>
      <nav class="flex-1 px-3 py-6">
        <button
          v-for="item in nav"
          :key="item.id"
          class="mb-1 flex w-full items-center gap-3 px-4 py-3 text-left text-sm transition"
          :class="
            activeView === item.id
              ? 'bg-signal-yellow font-semibold text-ink'
              : 'text-slate-300 hover:bg-white/10 hover:text-white'
          "
          @click="selectView(item.id)"
        >
          <span class="grid h-6 w-6 place-items-center text-base">{{
            item.icon
          }}</span
          >{{ item.label }}
        </button>
      </nav>
      <div class="border-t border-white/10 px-6 py-5 text-xs text-slate-400">
        <p class="mb-2 uppercase tracking-[.16em]">Flux actif</p>
        <div class="flex items-center gap-2 text-white">
          <i class="h-2 w-2 rounded-full bg-signal-green" /> Spark → HBase +
          HDFS
        </div>
        <p class="mt-2">Kafka transporte les données brutes uniquement.</p>
      </div>
    </aside>
    <div class="lg:pl-64">
      <header
        class="sticky top-0 z-10 flex min-h-20 items-center justify-between border-b border-line bg-white/95 px-5 py-4 backdrop-blur lg:px-9"
      >
        <div>
          <p class="text-[10px] font-bold tracking-[.18em] text-muted">
            SÉCURITÉ ROUTIÈRE · SUPERVISION
          </p>
          <h2 class="mt-1 text-xl font-semibold">
            {{ nav.find((item) => item.id === activeView)?.label }}
          </h2>
        </div>
        <div class="flex items-center gap-4">
          <span class="hidden items-center gap-2 text-xs text-muted sm:flex"
            ><i class="h-2 w-2 rounded-full bg-signal-green" />Flux
            nominal</span
          ><button
            class="border border-line px-3 py-2 text-xs font-semibold hover:bg-paper"
            :disabled="refreshing"
            @click="refresh"
          >
            {{ refreshing ? "Actualisation..." : "Actualiser" }}
          </button>
        </div>
      </header>
      <div class="border-b border-line bg-white px-4 py-3 lg:hidden">
        <div class="flex gap-2 overflow-x-auto">
          <button
            v-for="item in nav"
            :key="item.id"
            class="whitespace-nowrap px-3 py-2 text-xs"
            :class="
              activeView === item.id
                ? 'bg-ink text-white'
                : 'bg-paper text-muted'
            "
            @click="selectView(item.id)"
          >
            {{ item.label }}
          </button>
        </div>
      </div>
      <main class="mx-auto max-w-[1500px] px-4 py-6 lg:px-9 lg:py-8">
        <p
          v-if="errors.length"
          class="mb-5 border border-[#e9c98f] bg-[#fff3df] px-4 py-3 text-xs text-[#805816]"
        >
          Mode dégradé · {{ errors.join(" · ") }}
        </p>
        <template v-if="activeView === 'overview'">
          <section class="grid grid-cols-2 gap-3 md:grid-cols-4">
            <div
              v-for="card in [
                {
                  label: 'Incidents analysés',
                  value: rows.reduce(
                    (sum, row) => sum + Number(row.nb_incidents || 0),
                    0,
                  ),
                  hint: 'HBase + Hive',
                },
                {
                  label: 'Zones rouges',
                  value: redCount,
                  hint: 'À traiter maintenant',
                },
                {
                  label: 'Score moyen',
                  value: avgScore,
                  hint: 'sur les cellules visibles',
                },
                {
                  label: 'Score maximal',
                  value: maxScore,
                  hint: 'niveau de criticité',
                },
              ]"
              :key="card.label"
              class="border border-line bg-white p-5"
            >
              <span class="text-xs text-muted">{{ card.label }}</span
              ><strong class="mt-2 block font-display text-3xl font-semibold">{{
                card.value
              }}</strong
              ><small class="mt-2 block text-[11px] text-muted">{{
                card.hint
              }}</small>
            </div>
          </section>
          <section class="mt-5 grid gap-5 xl:grid-cols-[minmax(0,1.4fr)_380px]">
            <div class="border border-line bg-white p-5">
              <div class="mb-5 flex items-start justify-between">
                <div>
                  <p class="eyebrow">Priorité opérationnelle</p>
                  <h3 class="mt-1 text-xl font-semibold">Zones à surveiller</h3>
                </div>
                <button
                  class="text-xs font-semibold text-signal-orange"
                  @click="selectView('map')"
                >
                  Ouvrir la carte →
                </button>
              </div>
              <div v-if="rows.length" class="space-y-3">
                <button
                  v-for="row in rows.slice(0, 6)"
                  :key="row.id"
                  class="flex w-full items-center gap-4 border-b border-line pb-3 text-left last:border-0 last:pb-0"
                  @click="
                    selected = row;
                    selectView('map');
                  "
                >
                  <span
                    class="h-3 w-3 shrink-0 rounded-full"
                    :style="{ background: color(row.niveau_risque) }"
                  /><span class="min-w-0 flex-1"
                    ><b class="block truncate text-sm">{{ row.zone }}</b
                    ><small class="text-xs text-muted"
                      >{{ row.nb_incidents || 0 }} incidents ·
                      {{ row.source }}</small
                    ></span
                  ><span class="w-28"
                    ><span
                      class="mb-1 block text-right text-xs font-semibold"
                      >{{ Number(row.score_risque || 0).toFixed(1) }}</span
                    ><i
                      class="block h-1 bg-signal-orange"
                      :style="{ width: barWidth(row) }" /></span
                  ><em
                    class="w-16 text-right text-[10px] font-bold not-italic"
                    :style="{ color: color(row.niveau_risque) }"
                    >{{ label(row.niveau_risque) }}</em
                  >
                </button>
              </div>
              <p v-else class="py-10 text-center text-sm text-muted">
                Aucune cellule disponible.
              </p>
            </div>
            <div class="border border-line bg-ink p-5 text-white">
              <p class="eyebrow text-signal-yellow">Chaîne de traitement</p>
              <div class="mt-5 space-y-4">
                <div
                  v-for="step in [
                    { name: 'MinIO → NiFi', detail: 'Fichiers terrain' },
                    { name: 'NiFi → Kafka', detail: 'Topics raw' },
                    {
                      name: 'Spark Streaming',
                      detail: 'Scoring + anonymisation',
                    },
                    { name: 'HBase + HDFS', detail: 'Alertes + Gold' },
                  ]"
                  :key="step.name"
                  class="flex items-center gap-3"
                >
                  <i
                    class="grid h-7 w-7 place-items-center rounded-full border border-signal-yellow text-xs text-signal-yellow"
                    >✓</i
                  ><span
                    ><b class="block text-sm">{{ step.name }}</b
                    ><small class="text-xs text-slate-400">{{
                      step.detail
                    }}</small></span
                  >
                </div>
              </div>
            </div>
          </section>
        </template>
        <template v-else-if="activeView === 'map'"
          ><div class="mb-4 flex flex-wrap gap-2">
            <select
              v-model="riskFilter"
              class="border border-line bg-white px-3 py-2 text-xs"
            >
              <option value="ALL">Tous les niveaux</option>
              <option value="ROUGE">Rouge</option>
              <option value="ORANGE">Orange</option>
              <option value="VERT">Vert</option></select
            ><select
              v-model="zoneFilter"
              class="border border-line bg-white px-3 py-2 text-xs"
            >
              <option value="ALL">Toutes les zones</option>
              <option v-for="zone in zones" :key="zone">{{ zone }}</option>
            </select>
          </div>
          <section class="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
            <div class="border border-line bg-white">
              <div ref="mapElement" class="h-[560px] bg-[#dce9e5]" />
              <footer
                class="border-t border-line px-4 py-3 text-[11px] text-muted"
              >
                HBase · alertes opérationnelles &nbsp;|&nbsp; Hive · agrégat 24
                h &nbsp;|&nbsp; grille 2 km × 2 km
              </footer>
            </div>
            <div class="border border-line bg-white p-5">
              <p class="eyebrow">Cellule sélectionnée</p>
              <h3 class="mt-1 text-xl font-semibold">
                {{ selected?.zone || "Sélectionnez une cellule" }}
              </h3>
              <div v-if="selected" class="mt-6">
                <strong class="font-display text-5xl">{{
                  Number(selected.score_risque || 0).toFixed(1)
                }}</strong
                ><span
                  class="ml-3 px-2 py-1 text-[10px] font-bold text-white"
                  :style="{ background: color(selected.niveau_risque) }"
                  >{{ label(selected.niveau_risque) }}</span
                >
                <dl class="mt-6 grid grid-cols-2 gap-4 text-xs">
                  <div>
                    <dt class="text-muted">Incidents</dt>
                    <dd class="mt-1 font-semibold">
                      {{ selected.nb_incidents }}
                    </dd>
                  </div>
                  <div>
                    <dt class="text-muted">Victimes</dt>
                    <dd class="mt-1 font-semibold">
                      {{ selected.nb_victimes }}
                    </dd>
                  </div>
                  <div>
                    <dt class="text-muted">Cellule</dt>
                    <dd class="mt-1 font-semibold">
                      {{ selected.grid_2km_id || "N/A" }}
                    </dd>
                  </div>
                  <div>
                    <dt class="text-muted">Source</dt>
                    <dd class="mt-1 font-semibold">{{ selected.source }}</dd>
                  </div>
                </dl>
                <div
                  class="mt-7 border-l-2 border-signal-orange bg-[#fff8ec] p-3 text-xs"
                >
                  <b>Recommandation</b>
                  <p class="mt-1">
                    {{
                      selected.niveau_risque === "ROUGE"
                        ? "Renforcer la patrouille sur cette zone."
                        : "Maintenir la surveillance opérationnelle."
                    }}
                  </p>
                </div>
              </div>
              <p v-else class="mt-8 text-sm leading-6 text-muted">
                Cliquez sur un point pour consulter son score, sa source et la
                recommandation associée.
              </p>
            </div>
          </section></template
        >
        <template v-else-if="activeView === 'analytics'"
          ><section class="grid gap-5 lg:grid-cols-2">
            <div class="panel">
              <p class="eyebrow">Hive / HDFS Gold</p>
              <h3 class="panel-title">Risque par cellule sur 24 h</h3>
              <div class="mt-6 space-y-4">
                <div v-for="row in analytical_24h.slice(0, 8)" :key="row.id">
                  <div class="mb-1 flex justify-between text-xs">
                    <span>{{ row.zone }}</span
                    ><b>{{ Number(row.score_risque || 0).toFixed(1) }}</b>
                  </div>
                  <div class="h-2 bg-paper">
                    <i
                      class="block h-full"
                      :style="{
                        width: barWidth(row),
                        background: color(row.niveau_risque),
                      }"
                    />
                  </div>
                </div>
                <p
                  v-if="!analytical_24h.length"
                  class="py-10 text-center text-sm text-muted"
                >
                  Aucun agrégat Hive disponible.
                </p>
              </div>
            </div>
            <div class="panel">
              <p class="eyebrow">Synthèse temporelle</p>
              <h3 class="panel-title">Répartition des niveaux</h3>
              <div class="mt-8 grid grid-cols-3 gap-3 text-center">
                <div
                  v-for="level in ['ROUGE', 'ORANGE', 'VERT']"
                  :key="level"
                  class="border border-line p-4"
                >
                  <b
                    class="font-display text-3xl"
                    :style="{ color: color(level) }"
                    >{{
                      rows.filter((row) => row.niveau_risque === level).length
                    }}</b
                  ><span class="mt-2 block text-xs text-muted">{{
                    label(level)
                  }}</span>
                </div>
              </div>
              <div class="mt-8 border-t border-line pt-5 text-sm text-muted">
                Les agrégats sont produits par Spark puis déposés dans HDFS
                avant exposition par Hive.
              </div>
            </div>
          </section></template
        >
        <template v-else-if="activeView === 'alerts'"
          ><section class="panel">
            <div class="mb-5 flex items-center justify-between">
              <div>
                <p class="eyebrow">HBase · temps réel</p>
                <h3 class="panel-title">Alertes opérationnelles</h3>
              </div>
              <span class="text-xs text-muted"
                >{{ operational.length }} alertes</span
              >
            </div>
            <div class="overflow-x-auto">
              <table class="w-full text-left text-sm">
                <thead
                  class="border-b border-line text-[10px] uppercase tracking-wider text-muted"
                >
                  <tr>
                    <th class="pb-3">Zone</th>
                    <th class="pb-3">Niveau</th>
                    <th class="pb-3">Score</th>
                    <th class="pb-3">Incidents</th>
                    <th class="pb-3">Action</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="row in operational"
                    :key="row.id"
                    class="border-b border-line last:border-0"
                  >
                    <td class="py-4 font-semibold">{{ row.zone }}</td>
                    <td class="py-4">
                      <span
                        class="font-bold"
                        :style="{ color: color(row.niveau_risque) }"
                        >{{ label(row.niveau_risque) }}</span
                      >
                    </td>
                    <td class="py-4">
                      {{ Number(row.score_risque || 0).toFixed(1) }}
                    </td>
                    <td class="py-4">{{ row.nb_incidents || 0 }}</td>
                    <td class="py-4 text-xs text-muted">
                      {{
                        row.niveau_risque === "ROUGE"
                          ? "Renforcer la patrouille"
                          : "Maintenir la surveillance"
                      }}
                    </td>
                  </tr>
                </tbody>
              </table>
              <p
                v-if="!operational.length"
                class="py-10 text-center text-sm text-muted"
              >
                Aucune alerte HBase disponible.
              </p>
            </div>
          </section></template
        >
        <template v-else-if="activeView === 'ml'"
          ><section class="grid gap-5 lg:grid-cols-[.8fr_1.2fr]">
            <div class="panel">
              <p class="eyebrow">Airflow / Spark ML</p>
              <h3 class="panel-title">Modèle hotspot</h3>
              <div
                class="mt-6 border-l-2 border-signal-green bg-[#eef8f1] p-4 text-sm"
              >
                <b>Pipeline supervisé</b>
                <p class="mt-1 text-xs text-muted">
                  Entraînement et mise à jour orchestrés par Airflow.
                </p>
              </div>
              <dl class="mt-6 space-y-4 text-sm">
                <div class="flex justify-between border-b border-line pb-3">
                  <dt class="text-muted">Statut</dt>
                  <dd class="font-semibold text-signal-green">
                    Artefacts disponibles
                  </dd>
                </div>
                <div class="flex justify-between border-b border-line pb-3">
                  <dt class="text-muted">Variables</dt>
                  <dd class="font-semibold">Gravité · météo · véhicule</dd>
                </div>
                <div class="flex justify-between">
                  <dt class="text-muted">Sortie</dt>
                  <dd class="font-semibold">Score de risque</dd>
                </div>
              </dl>
            </div>
            <div class="panel">
              <p class="eyebrow">Décision terrain</p>
              <h3 class="panel-title">Recommandations de patrouille</h3>
              <div class="mt-5 divide-y divide-line">
                <div
                  v-for="row in rows.slice(0, 8)"
                  :key="row.id"
                  class="flex items-center justify-between py-3"
                >
                  <span
                    ><b class="block text-sm">{{ row.zone }}</b
                    ><small class="text-xs text-muted"
                      >Score
                      {{ Number(row.score_risque || 0).toFixed(1) }}</small
                    ></span
                  ><span class="text-right text-xs font-semibold">{{
                    row.niveau_risque === "ROUGE" ? "Renforcer" : "Surveiller"
                  }}</span>
                </div>
              </div>
            </div>
          </section></template
        >
        <template v-else
          ><section class="grid gap-5 lg:grid-cols-2">
            <div class="panel">
              <p class="eyebrow">Traçabilité du flux</p>
              <h3 class="panel-title">Qualité et confidentialité</h3>
              <div class="mt-6 grid grid-cols-2 gap-3">
                <div
                  v-for="item in [
                    { label: 'Sources', value: '4' },
                    { label: 'Topics raw', value: '2' },
                    { label: 'HBase', value: operational.length },
                    { label: 'Hive', value: analytical_24h.length },
                  ]"
                  :key="item.label"
                  class="border border-line p-4"
                >
                  <b class="font-display text-2xl">{{ item.value }}</b
                  ><span class="mt-1 block text-xs text-muted">{{
                    item.label
                  }}</span>
                </div>
              </div>
            </div>
            <div class="panel">
              <p class="eyebrow">Privacy by Design</p>
              <h3 class="panel-title">Sorties anonymisées</h3>
              <div
                class="mt-6 flex items-center gap-4 border border-[#b8dcc3] bg-[#eef8f1] p-5"
              >
                <span
                  class="grid h-10 w-10 place-items-center rounded-full bg-signal-green text-xl text-white"
                  >✓</span
                >
                <div>
                  <b class="block">Aucune PII exposée</b
                  ><span class="text-xs text-muted"
                    >incident_id, nom_victime et tel_temoin sont supprimés avant
                    HBase/HDFS.</span
                  >
                </div>
              </div>
              <div class="mt-5 text-xs text-muted">
                Le dashboard ne lit jamais les topics Kafka de sortie : il
                consulte les stockages finaux.
              </div>
            </div>
          </section></template
        >
      </main>
    </div>
  </div>
</template>
