import { createApp } from 'vue'
import { VueQueryPlugin } from '@tanstack/vue-query'
import { router } from './router'
import App from './App.vue'
import './style.css'

const app = createApp(App)

app.use(VueQueryPlugin)
app.use(router)

app.mount('#app')
