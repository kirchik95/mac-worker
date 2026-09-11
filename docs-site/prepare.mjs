import { readdir, readFile, writeFile, mkdir, rm, copyFile } from 'node:fs/promises'
import { execFileSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const site = path.dirname(fileURLToPath(import.meta.url))
const repo = path.resolve(site, '..')
const content = path.join(site, 'content')
const trackedFiles = execFileSync('git', ['ls-files', '-z'], { cwd: repo, encoding: 'utf8' }).split('\0').filter(Boolean)
const russianArchives = new Set([
  'docs/2026-09-08-architecture-and-flow-review.md',
  'docs/superpowers/plans/2026-09-08-pool-reliability-roadmap.md',
  'docs/superpowers/plans/2026-09-08-cursor-auth-profile.md',
])
const sources = new Map([
  ['README.md', 'index.md'],
  ['README.ru.md', 'ru/index.md'],
  ['ui/README.md', 'development/dashboard.md'],
])

async function collect(directory) {
  for (const entry of await readdir(path.join(repo, directory), { withFileTypes: true })) {
    if (entry.name.startsWith('.')) continue
    const relative = `${directory}/${entry.name}`
    if (entry.isDirectory()) await collect(relative)
    else if (entry.isFile()) sources.set(relative, russianArchives.has(relative) ? `ru/${relative}` : relative)
  }
}
await collect('docs')

async function rewriteLink(url, source, revision) {
  if (!url || /^(?:[a-z][a-z\d+.-]*:|\/\/|#)/i.test(url)) return url
  const [, pathname, suffix = ''] = url.match(/^([^?#]*)(.*)$/)
  const localReference = pathname.match(/^\/.*\/mac-worker\/(.+?)(?::(\d+))?$/)
  if (localReference) {
    const line = localReference[2] ? `#L${localReference[2]}` : suffix
    return `https://github.com/kirchik95/mac-worker/blob/${revision}/${localReference[1]}${line}`
  }
  const target = path.resolve(repo, path.dirname(source), decodeURI(pathname))
  const relative = path.relative(repo, target).split(path.sep).join('/')
  if (sources.has(relative)) return `/${sources.get(relative)}${suffix}`
  if (relative === 'LICENSE') return `/license.md${suffix}`
  if (relative === 'docs/superpowers/specs' || relative === 'docs/superpowers/plans') {
    return '/archive.md'
  }
  if (!relative.startsWith('../') && !path.isAbsolute(relative)) {
    const tracked = trackedFiles.includes(relative)
    const directory = trackedFiles.some(name => name.startsWith(`${relative}/`))
    if (tracked || directory) {
      const kind = directory ? 'tree' : 'blob'
      return `https://github.com/kirchik95/mac-worker/${kind}/main/${relative}${suffix}`
    }
  }
  return url
}

async function transformProse(line, source, revision) {
  line = line.replace(/<!--.*?-->/g, '')
  // Markdown images receive VitePress's asset/base-path handling.
  line = line.replace(/<img\b([^>]+)>/g, (tag, attrs) => {
    const src = attrs.match(/src="([^"]+)"/)?.[1]
    const alt = attrs.match(/alt="([^"]*)"/)?.[1] ?? ''
    return src ? `![${alt}](${src})` : tag
  }).replace(/<\/?p(?:\s+align="center")?>/g, '')
  const pattern = /(!?\[[^\]\n]*\]\()([^\s)]+)([^)]*\))/g
  for (const match of [...line.matchAll(pattern)].reverse()) {
    const replacement = match[1] + await rewriteLink(match[2], source, revision) + match[3]
    line = line.slice(0, match.index) + replacement + line.slice(match.index + match[0].length)
  }
  return line
}

async function transform(text, source) {
  const result = []
  const revision = text.match(/HEAD\s+`([a-f\d]{7,40})`/i)?.[1] ?? 'main'
  let fence = null
  for (let line of text.split('\n')) {
    const marker = line.match(/^\s*(`{3,}|~{3,})/)
    if (marker) {
      if (!fence) fence = marker[1]
      else if (marker[1][0] === fence[0] && marker[1].length >= fence.length) fence = null
      result.push(line)
      continue
    }
    if (!fence) {
      const codeSpans = []
      const masked = line.replace(/(`+)(.*?)\1(?!`)/g, span => {
        codeSpans.push(span)
        return `\u0000${codeSpans.length - 1}\u0000`
      })
      line = (await transformProse(masked, source, revision)).replace(/\u0000(\d+)\u0000/g, (_, index) => codeSpans[Number(index)])
    }
    result.push(line)
  }
  return result.join('\n')
}

await rm(content, { recursive: true, force: true })
await mkdir(content, { recursive: true })
for (const [source, destination] of sources) {
  const output = path.join(content, destination)
  await mkdir(path.dirname(output), { recursive: true })
  if (source.endsWith('.md')) {
    let text = await transform(await readFile(path.join(repo, source), 'utf8'), source)
    const frontmatter = text.match(/^---\r?\n[\s\S]*?\r?\n---\r?\n/)?.[0] ?? ''
    let body = text.slice(frontmatter.length)
    const russian = russianArchives.has(source)
    if (source.startsWith('docs/superpowers/') || /^(?:\d{4}-\d{2}-\d{2}-|pool-run-)|(?:validation|spike|acceptance-runbook)\.md$/.test(path.basename(source))) {
      body = (russian
        ? '::: info Архив проекта\nЗапись описывает работу на указанную дату. Текущее поведение — в [справочнике](/docs/usage.md).\n:::\n\n'
        : '::: info Engineering archive\nThese notes describe work at the date shown. See the [usage reference](/docs/usage.md) for current behavior.\n:::\n\n') + body
    }
    text = frontmatter + body
    await writeFile(output, text)
  } else await copyFile(path.join(repo, source), output)
}
const archive = [...sources.keys()].filter(name => name.startsWith('docs/') && name.endsWith('.md'))
await writeFile(path.join(content, 'archive.md'), '# Documentation index\n\nAll guides, design documents, implementation plans and validation records.\n\n' + (await Promise.all(archive.sort().map(async name => {
  const title = (await readFile(path.join(repo, name), 'utf8')).match(/^#\s+(.+)$/m)?.[1] ?? path.basename(name, '.md')
  return `- [${title.replace(/[\[\]]/g, '')}](/${sources.get(name)})`
}))).join('\n') + '\n')
await writeFile(path.join(content, 'license.md'), '# MIT License\n\n```text\n' + await readFile(path.join(repo, 'LICENSE'), 'utf8') + '\n```\n')
await mkdir(path.join(content, 'public/assets'), { recursive: true })
for (const font of ['plex-sans-regular.ttf', 'plex-sans-medium.ttf', 'plex-mono-regular.ttf']) {
  await copyFile(path.join(repo, 'src/dashboard/static/app/assets', font), path.join(content, 'public/assets', font))
}
await copyFile(path.join(repo, 'ui/public/favicon.svg'), path.join(content, 'public/favicon.svg'))
await copyFile(path.join(repo, 'ui/src/assets/fonts/OFL.txt'), path.join(content, 'public/assets/OFL.txt'))
console.log(`Prepared ${sources.size} source files; README and docs remain the source of truth.`)
