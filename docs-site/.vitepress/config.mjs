import { withMermaid } from 'vitepress-plugin-mermaid'

const guides = [
  { text: 'Quick start', link: '/' },
  { text: 'Complete getting started guide', link: '/docs/getting-started' },
  { text: 'Prepare a worker Mac', link: '/docs/setup-macos-worker' },
  { text: 'Usage reference', link: '/docs/usage' },
  { text: 'Installation recovery', link: '/docs/setup-recovery' },
]
const architecture = [
  { text: 'Remote controller', link: '/docs/usage#remote-controller' },
  { text: 'Task dependencies (DAG)', link: '/docs/dag-design' },
  { text: 'Execution slots', link: '/docs/superpowers/specs/2026-09-10-slots-design' },
  { text: 'Origin delivery', link: '/docs/superpowers/specs/2026-09-10-origin-outbox' },
]

export default withMermaid({
  srcDir: 'content',
  base: '/mac-worker/',
  title: 'mac-worker',
  description: 'Run coding agents on your Macs. Installation, task workflows and operations.',
  cleanUrls: false,
  // Keep diagram registries lazy; VitePress also preloads dynamic app imports by default.
  shouldPreload: link => !link.includes('/chunks/') || /\/(?:framework|theme)\./.test(link),
  markdown: {
    html: false,
    anchor: { slugify: text => text.toLowerCase().replace(/[^\p{L}\p{N}\p{M}_ -]/gu, '').replace(/ /g, '-') },
  },
  sitemap: { hostname: 'https://kirchik95.github.io/mac-worker/' },
  head: [['link', { rel: 'icon', type: 'image/svg+xml', href: '/mac-worker/favicon.svg' }]],
  locales: {
    root: {
      label: 'English', lang: 'en', link: '/',
      themeConfig: {
        nav: [{ text: 'Guide', link: '/docs/getting-started' }, { text: 'Reference', link: '/docs/usage' }, { text: 'All docs', link: '/archive' }],
        sidebar: [
          { text: 'Start here', items: guides },
          { text: 'Pool & architecture', collapsed: false, items: architecture },
          { text: 'Maintain & develop', items: [
            { text: 'Update and remove', link: '/docs/getting-started#update-or-remove' },
            { text: 'Build and release', link: '/docs/releasing' },
            { text: 'Dashboard development', link: '/development/dashboard' },
            { text: 'Documentation site', link: '/docs/documentation-site' },
            { text: 'All documents & archive', link: '/archive' },
          ] },
        ],
        outline: { label: 'On this page', level: [2, 3] },
      },
    },
    ru: {
      label: 'Русский', lang: 'ru', link: '/ru/',
      title: 'mac-worker', description: 'Агенты для разработки на ваших Mac: установка, задачи и эксплуатация.',
      themeConfig: {
        nav: [{ text: 'Быстрый старт', link: '/ru/' }, { text: 'Справочник (EN)', link: '/docs/usage' }, { text: 'Все документы', link: '/archive' }],
        sidebar: [
          { text: 'Начало работы', items: [{ text: 'Быстрый старт', link: '/ru/' }] },
          { text: 'Полные руководства (EN)', items: [
            { text: 'Установка и первая задача', link: '/docs/getting-started' },
            { text: 'Настройка рабочего Mac', link: '/docs/setup-macos-worker' },
            { text: 'Команды и настройки', link: '/docs/usage' },
            { text: 'Удалённый контроллер', link: '/docs/usage#remote-controller' },
            { text: 'Обновление', link: '/docs/getting-started#update-or-remove' },
            { text: 'Все документы и архив', link: '/archive' },
          ] },
        ],
        outline: { label: 'На этой странице', level: [2, 3] },
        docFooter: { prev: 'Назад', next: 'Далее' },
        sidebarMenuLabel: 'Навигация', returnToTopLabel: 'Наверх',
        darkModeSwitchLabel: 'Тема', lightModeSwitchTitle: 'Светлая тема', darkModeSwitchTitle: 'Тёмная тема',
      },
    },
  },
  themeConfig: {
    // Only the quick start is translated; switch to each language's landing page.
    i18nRouting: false,
    siteTitle: 'mac-worker / docs',
    socialLinks: [{ icon: 'github', link: 'https://github.com/kirchik95/mac-worker' }],
    search: { provider: 'local', options: { locales: { ru: { translations: {
      button: { buttonText: 'Поиск', buttonAriaLabel: 'Поиск по документации' },
      modal: { noResultsText: 'Ничего не найдено', resetButtonTitle: 'Очистить', footer: { selectText: 'выбрать', navigateText: 'перейти', closeText: 'закрыть' } },
    } } } } },
    footer: { message: '<a href="/mac-worker/license.html">MIT License</a> · <a href="/mac-worker/assets/OFL.txt">Font license</a>' },
  },
  mermaid: { securityLevel: 'strict' },
})
