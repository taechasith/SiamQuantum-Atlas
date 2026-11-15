import { createRouter, createWebHistory } from 'vue-router'
import AnalyticsPage from '@/pages/AnalyticsPage.vue'
import DatabasePage from '@/pages/DatabasePage.vue'
import SubmitPage from '@/pages/SubmitPage.vue'
import ProfilePage from '@/pages/ProfilePage.vue'
import AdminPage from '@/pages/AdminPage.vue'

const AtlasPage = () => import('@/pages/AtlasPage.vue')
const AboutPage = () => import('@/pages/AboutPage.vue')
const DesignSystemPage = () => import('@/pages/DesignSystemPage.vue')

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/atlas' },
    { path: '/home', redirect: '/atlas' },
    { path: '/dashboard', name: 'dashboard', component: AnalyticsPage },
    { path: '/about', name: 'about', component: AboutPage },
    { path: '/ci', name: 'ci', component: DesignSystemPage },
    { path: '/atlas', name: 'atlas', component: AtlasPage },
    { path: '/map', redirect: { path: '/atlas', query: { view: 'globe' } } },
    { path: '/network', redirect: { path: '/atlas', query: { view: 'network' } } },
    { path: '/analytics', redirect: '/dashboard' },
    { path: '/database', name: 'database', component: DatabasePage },
    { path: '/submit-data', name: 'submit-data', component: SubmitPage },
    { path: '/profile', name: 'profile', component: ProfilePage },
    { path: '/admin', name: 'admin', component: AdminPage, meta: { requiresAdmin: true } },
    { path: '/:pathMatch(.*)*', redirect: '/atlas' },
  ],
})

router.beforeEach(async (to) => {
  if (!to.meta.requiresAdmin) return true
  try {
    const res = await fetch('/api/auth/me')
    if (!res.ok) return { path: '/atlas' }
    const json = await res.json()
    const role = json?.data?.profile?.role ?? json?.data?.user?.role
    return role === 'admin' ? true : { path: '/atlas' }
  } catch {
    return { path: '/atlas' }
  }
})
