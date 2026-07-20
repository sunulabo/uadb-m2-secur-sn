import { createApp, h } from 'vue'
import { createInertiaApp } from '@inertiajs/vue3'
import './style.css'
createInertiaApp({
  resolve: name => import.meta.glob('./Pages/**/*.vue', { eager: true })[`./Pages/${name}.vue`],
  setup({ el, App, props, plugin }) { createApp({ render: () => h(App, props) }).use(plugin).mount(el) }
})
