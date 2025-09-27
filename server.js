const express = require('express')
const crypto = require('crypto')
const { URL } = require('url')
const cheerio = require('cheerio')
const { Readable } = require('stream')
const vm = require('vm')
const { spawn } = require('child_process')
const os = require('os')
const path = require('path')
const fs = require('fs')

const app = express()
app.set('etag', false)
app.use(express.json())
app.use(express.urlencoded({ extended: false }))
app.use((req, res, next) => {
  const t = Date.now()
  res.on('finish', () => console.log(`${req.method} ${req.originalUrl} -> ${res.statusCode} ${Date.now() - t}ms`))
  next()
})

const CACHE_ROOT = path.join(os.tmpdir(), 'ap-transmux')
fs.mkdirSync(CACHE_ROOT, { recursive: true })

const cacheStatic = express.static(CACHE_ROOT, { etag: false, lastModified: false, cacheControl: false, fallthrough: true })
app.use('/cache', (req, res, next) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
  res.setHeader('Pragma', 'no-cache')
  res.setHeader('Expires', '0')
  if (req.path.endsWith('.m3u8')) res.type('application/vnd.apple.mpegurl')
  if (req.path.endsWith('.m4s')) res.type('video/iso.segment')
  cacheStatic(req, res, next)
})

app.use(express.static('public', { etag: false, lastModified: false }))

const HOST = 'https://animepahe.si'
const API_URL = `${HOST}/api`
const REFERER = HOST
const tokenStore = new Map()
const procs = new Map()

const DEFAULT_HEADERS = {
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
  'accept': '*/*',
  'accept-language': 'en-US,en;q=0.9',
  'cache-control': 'no-cache',
  'pragma': 'no-cache'
}

function genCookie() { return `__ddg2_=${crypto.randomBytes(12).toString('hex')}` }
function saveToken(data) { const t = crypto.randomBytes(16).toString('hex'); tokenStore.set(t, { ...data, createdAt: Date.now() }); setTimeout(() => tokenStore.delete(t), 60 * 60 * 1000); return t }
function getToken(t) { return tokenStore.get(t) }
function mergeHeaders(h1 = {}, h2 = {}) { return { ...DEFAULT_HEADERS, ...h1, ...h2 } }

async function httpGet(url, { headers = {}, signal } = {}) {
  const res = await fetch(url, { headers: mergeHeaders(headers), redirect: 'follow', signal })
  if (!res.ok) throw new Error(`GET ${url} ${res.status}`)
  return res
}
async function httpGetRaw(url, { headers = {}, signal } = {}) { return fetch(url, { headers: mergeHeaders(headers), redirect: 'follow', signal }) }
async function httpText(url, opts) { const res = await httpGet(url, opts); return await res.text() }

async function searchAnime(q, cookie) {
  const url = `${API_URL}?m=search&q=${encodeURIComponent(q)}`
  const res = await httpGet(url, { headers: { cookie } })
  return await res.json()
}
async function getReleasePage(slug, page, cookie) {
  const url = `${API_URL}?m=release&id=${encodeURIComponent(slug)}&sort=episode_asc&page=${page}`
  const res = await httpGet(url, { headers: { cookie } })
  return await res.json()
}
async function getAllEpisodes(slug, cookie) {
  const first = await getReleasePage(slug, 1, cookie)
  let data = first.data || []
  const last = first.last_page || 1
  if (last > 1) {
    const tasks = []
    for (let p = 2; p <= last; p++) tasks.push(getReleasePage(slug, p, cookie))
    const pages = await Promise.all(tasks)
    for (const pg of pages) data = data.concat(pg.data || [])
  }
  data.sort((a, b) => Number(a.episode) - Number(b.episode))
  return data
}

function collectButtons($) {
  const seen = new Set()
  const out = []
  $('button[data-src]').each((_, el) => {
    const e = $(el)
    const audio = (e.attr('data-audio') || '').toLowerCase()
    const resolution = e.attr('data-resolution') || ''
    const av1 = e.attr('data-av1') || ''
    const src = e.attr('data-src') || ''
    const key = `${audio}|${resolution}|${av1}|${src}`
    if (src && !seen.has(key)) { seen.add(key); out.push({ audio, resolution, av1, src }) }
  })
  out.sort((a, b) => {
    const av1a = a.av1 === '0' ? 0 : 1
    const av1b = b.av1 === '0' ? 0 : 1
    if (av1a !== av1b) return av1a - av1b
    return Number(b.resolution || 0) - Number(a.resolution || 0)
  })
  return out
}
function pickButton($, pref) {
  const buttons = collectButtons($)
  if (!buttons.length) return null
  let pool = buttons
  if (pref.audio) { const f = pool.filter(x => x.audio === pref.audio.toLowerCase()); pool = f.length ? f : pool }
  if (pref.resolution) { const f = pool.filter(x => x.resolution === String(pref.resolution)); pool = f.length ? f : pool }
  return pool[0] || null
}

function extractEvalScript(html) {
  const $ = cheerio.load(html)
  const scripts = $('script').map((_, s) => $(s).html() || '').get()
  for (const sc of scripts) {
    if (!sc) continue
    if (sc.includes('eval(')) return sc
    if (sc.includes('source=') && sc.includes('.m3u8')) return sc
  }
  return ''
}
function transformEvalScript(sc) { return sc.replace(/document/g, 'process').replace(/window/g, 'globalThis').replace(/querySelector/g, 'exit').replace(/eval\(/g, 'console.log(') }
function parseSourceFromLogs(out) {
  const lines = out.split('\n')
  for (const line of lines) {
    const m = line.match(/(?:var|let|const)\s+source\s*=\s*['"]([^'"]+\.m3u8)['"]/)
    if (m) return m[1]
    const any = line.match(/https?:\/\/[^\s'"]+\.m3u8/)
    if (any) return any[0]
  }
  return ''
}

async function getEpisodeM3U8({ slug, episode, audio, resolution, cookie }) {
  const episodes = await getAllEpisodes(slug, cookie)
  const ep = episodes.find(e => Number(e.episode) === Number(episode))
  if (!ep) return ''
  const playUrl = `${HOST}/play/${encodeURIComponent(slug)}/${ep.session}`
  const html = await httpText(playUrl, { headers: { cookie, Referer: REFERER } })
  const $ = cheerio.load(html)
  const btn = pickButton($, { audio, resolution })
  if (!btn) return ''
  const kwik = btn.src
  const kwikHtml = await httpText(kwik, { headers: { cookie, Referer: REFERER } })
  const raw = extractEvalScript(kwikHtml)
  if (!raw) return ''
  const transformed = transformEvalScript(raw)
  let output = ''
  const context = { console: { log: (...a) => { output += a.join(' ') + '\n' } }, atob: (b) => Buffer.from(b, 'base64').toString('binary'), btoa: (s) => Buffer.from(s, 'binary').toString('base64'), process: {}, globalThis: {}, navigator: { userAgent: DEFAULT_HEADERS['user-agent'] } }
  try { vm.createContext(context); new vm.Script(transformed).runInContext(context, { timeout: 2000 }) } catch {}
  const m3u8 = parseSourceFromLogs(output)
  return m3u8
}

function absUrl(u, base) {
  try {
    if (/^https?:\/\//i.test(u)) return u
    if (/^\/\//.test(u)) return 'https:' + u
    return new URL(u, base).href
  } catch { return u }
}
function shouldProxyAsPlaylist(u) { return /\.m3u8(\?|$)/i.test(u) }

function rewritePlaylist(content, base, token) {
  const lines = content.split('\n')
  const out = []
  let pendingStreamInf = null
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    if (line.startsWith('#EXT-X-STREAM-INF')) {
      pendingStreamInf = line
      const lower = line.toLowerCase()
      const isAv1 = lower.includes('codecs="av01') || lower.includes('codecs="av1')
      if (isAv1) pendingStreamInf = { drop: true }
      continue
    }
    if (pendingStreamInf) {
      if (pendingStreamInf.drop) { pendingStreamInf = null; continue }
      const tag = pendingStreamInf
      const url = absUrl(line.trim(), base)
      const prox = `/proxy/playlist?token=${encodeURIComponent(token)}&url=${encodeURIComponent(url)}&ref=${encodeURIComponent(base)}`
      out.push(tag)
      out.push(prox)
      pendingStreamInf = null
      continue
    }
    if (line.startsWith('#EXT-X-I-FRAME-STREAM-INF')) {
      const m = line.match(/URI="([^"]+)"/)
      if (m) {
        const abs = absUrl(m[1], base)
        const prox = `/proxy/playlist?token=${encodeURIComponent(token)}&url=${encodeURIComponent(abs)}&ref=${encodeURIComponent(base)}`
        out.push(line.replace(m[1], prox))
      } else out.push(line)
      continue
    }
    if (line.startsWith('#EXT-X-MAP')) {
      const m = line.match(/URI="([^"]+)"/)
      if (m) {
        const abs = absUrl(m[1], base)
        const prox = `/proxy/segment?token=${encodeURIComponent(token)}&url=${encodeURIComponent(abs)}&ref=${encodeURIComponent(base)}`
        out.push(line.replace(m[1], prox))
      } else out.push(line)
      continue
    }
    if (line.startsWith('#EXT-X-KEY')) {
      const m = line.match(/URI="([^"]+)"/)
      if (m) {
        const abs = absUrl(m[1], base)
        const prox = `/proxy/key?token=${encodeURIComponent(token)}&url=${encodeURIComponent(abs)}&ref=${encodeURIComponent(base)}`
        out.push(line.replace(m[1], prox))
      } else out.push(line)
      continue
    }
    if (line.startsWith('#EXT-X-MEDIA')) {
      const m = line.match(/URI="([^"]+)"/)
      if (m) {
        const abs = absUrl(m[1], base)
        const prox = shouldProxyAsPlaylist(abs)
          ? `/proxy/playlist?token=${encodeURIComponent(token)}&url=${encodeURIComponent(abs)}&ref=${encodeURIComponent(base)}`
          : `/proxy/segment?token=${encodeURIComponent(token)}&url=${encodeURIComponent(abs)}&ref=${encodeURIComponent(base)}`
        out.push(line.replace(m[1], prox))
      } else out.push(line)
      continue
    }
    if (line.startsWith('#')) { out.push(line); continue }
    if (!line.trim()) { out.push(line); continue }
    const abs = absUrl(line.trim(), base)
    if (shouldProxyAsPlaylist(abs)) {
      const prox = `/proxy/playlist?token=${encodeURIComponent(token)}&url=${encodeURIComponent(abs)}&ref=${encodeURIComponent(base)}`
      out.push(prox)
    } else {
      const prox = `/proxy/segment?token=${encodeURIComponent(token)}&url=${encodeURIComponent(abs)}&ref=${encodeURIComponent(base)}`
      out.push(prox)
    }
  }
  return out.join('\n')
}

function pipeFetchResponse(r, res, urlForDebug) {
  const ct = r.headers.get('content-type')
  const cl = r.headers.get('content-length')
  const ar = r.headers.get('accept-ranges')
  res.setHeader('X-Upstream-Status', String(r.status))
  res.setHeader('X-Upstream-CT', ct || '')
  res.setHeader('X-Upstream-URL', urlForDebug || '')
  if (ct) res.setHeader('Content-Type', ct)
  if (cl) res.setHeader('Content-Length', cl)
  if (ar) res.setHeader('Accept-Ranges', ar)
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
  res.status(r.status)
  if (!r.body) return res.end()
  if (Readable.fromWeb) Readable.fromWeb(r.body).pipe(res)
  else r.arrayBuffer().then(b => res.send(Buffer.from(b))).catch(() => res.status(500).end())
}

function buildUpstreamHeaders({ cookie, ref, req }) {
  const headers = {}
  if (cookie) headers.cookie = cookie
  if (ref) headers.Referer = ref
  try { if (ref) headers.Origin = new URL(ref).origin } catch {}
  if (req.headers.range) headers.Range = req.headers.range
  if (req.headers['accept']) headers.Accept = req.headers['accept']
  if (req.headers['accept-language']) headers['Accept-Language'] = req.headers['accept-language']
  return headers
}

function exists(p) { try { return fs.statSync(p).isFile() } catch { return false } }
function waitForFile(p, timeoutMs = 20000, intervalMs = 300) {
  return new Promise(r => {
    const t0 = Date.now()
    const i = setInterval(() => {
      try { if (fs.statSync(p).isFile()) { clearInterval(i); r(true); return } } catch {}
      if (Date.now() - t0 > timeoutMs) { clearInterval(i); r(false) }
    }, intervalMs)
  })
}
function countSegmentsInPlaylist(p) {
  try {
    const txt = fs.readFileSync(p, 'utf8')
    return (txt.match(/#EXTINF:/g) || []).length
  } catch { return 0 }
}
async function waitForSegments(playlistPath, minSeg = Number(process.env.PREPARE_MIN_SEGMENTS || 6), timeoutMs = 20000, intervalMs = 400) {
  const t0 = Date.now()
  return await new Promise(res => {
    const tick = () => {
      const n = countSegmentsInPlaylist(playlistPath)
      if (n >= minSeg) return res(true)
      if (Date.now() - t0 > timeoutMs) return res(false)
      setTimeout(tick, intervalMs)
    }
    tick()
  })
}

function ensureTransmuxStart(m3u8Url, lang, reso) {
  const keyBase = `${m3u8Url}|${lang || ''}|${reso || ''}|event`
  const key = crypto.createHash('md5').update(keyBase).digest('hex')
  const outDir = path.join(CACHE_ROOT, key)
  const outM3U8 = path.join(outDir, 'stream.m3u8')
  const outMaster = path.join(outDir, 'master.m3u8')
  if (!procs.has(key) && !exists(outMaster)) {
    fs.mkdirSync(outDir, { recursive: true })
    const args = [
      '-loglevel','error',
      '-user_agent', DEFAULT_HEADERS['user-agent'],
      '-headers', `Referer: ${m3u8Url}\r\n`,
      '-i', m3u8Url,
      '-map','v:0','-c:v','copy',
      '-map','a:0','-c:a','aac','-profile:a','aac_low','-ac','2','-b:a','128k',
      '-hls_time','3',
      '-hls_list_size','0',
      '-hls_segment_type','fmp4',
      '-hls_flags','append_list+independent_segments+omit_endlist+temp_file',
      '-hls_playlist_type','event',
      '-hls_segment_filename', path.join(outDir,'seg-%04d.m4s'),
      '-master_pl_name','master.m3u8',
      outM3U8
    ]
    const ff = spawn('ffmpeg', args, { stdio: ['ignore','ignore','inherit'] })
    ff.on('close', () => { procs.delete(key) })
    procs.set(key, ff)
  }
  return { key, url: `/cache/${key}/master.m3u8`, masterPath: outMaster, mediaPath: outM3U8 }
}

app.get('/api/search', async (req, res) => {
  try { const q = String(req.query.q || '').trim(); if (!q) return res.status(400).json({ error: 'missing q' }); const cookie = genCookie(); const data = await searchAnime(q, cookie); res.json(data) }
  catch { res.status(500).json({ error: 'search_failed' }) }
})
app.get('/api/anime/:slug/episodes', async (req, res) => {
  try { const slug = req.params.slug; const cookie = genCookie(); const data = await getAllEpisodes(slug, cookie); res.json({ data }) }
  catch { res.status(500).json({ error: 'episodes_failed' }) }
})
app.get('/api/options/:slug/:episode', async (req, res) => {
  try {
    const slug = req.params.slug
    const episode = req.params.episode
    const cookie = genCookie()
    const episodes = await getAllEpisodes(slug, cookie)
    const ep = episodes.find(e => Number(e.episode) === Number(episode))
    if (!ep) return res.status(404).json({ error: 'not_found' })
    const playUrl = `${HOST}/play/${encodeURIComponent(slug)}/${ep.session}`
    const html = await httpText(playUrl, { headers: { cookie, Referer: REFERER } })
    const $ = cheerio.load(html)
    const seen = new Set()
    const options = []
    $('button[data-src]').each((_, el) => {
      const e = $(el)
      const audio = (e.attr('data-audio') || '').toLowerCase()
      const resolution = (e.attr('data-resolution') || '')
      const key = `${audio}|${resolution}`
      if (!seen.has(key)) { seen.add(key); options.push({ audio, resolution }) }
    })
    res.json({ options })
  } catch { res.status(500).json({ error: 'options_failed' }) }
})
app.get('/api/anime/:slug/meta', async (req, res) => {
  try {
    const slug = req.params.slug
    const cookie = genCookie()
    const html = await httpText(`${HOST}/anime/${encodeURIComponent(slug)}`, { headers: { cookie, Referer: REFERER } })
    const $ = cheerio.load(html)
    const title = $('meta[property="og:title"]').attr('content') || $('title').text().trim()
    const poster = $('meta[property="og:image"]').attr('content') || ''
    res.json({ title, poster })
  } catch { res.status(500).json({ error: 'meta_failed' }) }
})
app.get('/img', async (req, res) => {
  try {
    const url = String(req.query.url || '')
    if (!url) return res.status(400).send('missing url')
    let ref = ''
    try { ref = new URL(url).origin } catch {}
    const r = await fetch(url, { headers: mergeHeaders({ Referer: ref || REFERER, Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8' }), redirect: 'follow' })
    if (!r.ok) return res.status(r.status).end()
    const ct = r.headers.get('content-type') || ''
    const isImage = ct.toLowerCase().startsWith('image/')
    res.setHeader('Content-Type', isImage ? ct : 'image/jpeg')
    res.setHeader('Cache-Control', 'no-store')
    if (!r.body) return res.end()
    if (Readable.fromWeb) Readable.fromWeb(r.body).pipe(res)
    else { const buf = Buffer.from(await r.arrayBuffer()); res.end(buf) }
  } catch { res.status(500).end() }
})

app.get('/watch/:slug/:episode/master.m3u8', async (req, res) => {
  try {
    const slug = req.params.slug
    const episode = req.params.episode
    const audio = req.query.audio ? String(req.query.audio) : ''
    const resolution = req.query.resolution ? String(req.query.resolution) : ''
    const transmux = req.query.transmux === '1'
    const sid = String(req.query.sid || '')
    const cookie = genCookie()
    const m3u8 = await getEpisodeM3U8({ slug, episode, audio, resolution, cookie })
    if (!m3u8) return res.status(404).send('not found')
    if (transmux) {
      const r = ensureTransmuxStart(m3u8, audio, resolution)
      await waitForFile(r.masterPath, 20000, 300)
      await waitForSegments(r.mediaPath, Number(process.env.PREPARE_MIN_SEGMENTS || 6), 20000, 400)
      const cacheUrl = sid ? `${r.url}?sid=${encodeURIComponent(sid)}` : r.url
      return res.redirect(302, cacheUrl)
    }
    const token = saveToken({ cookie })
    const text = await httpText(m3u8, { headers: { cookie, Referer: REFERER } })
    const rewritten = rewritePlaylist(text, m3u8, token)
    res.setHeader('Content-Type', 'application/vnd.apple.mpegurl')
    res.send(rewritten)
  } catch { res.status(500).send('error') }
})

app.get('/proxy/playlist', async (req, res) => {
  try {
    const token = String(req.query.token || '')
    const url = String(req.query.url || '')
    const ref = String(req.query.ref || '')
    const t = getToken(token)
    if (!t) return res.status(403).send('forbidden')
    const r = await httpGetRaw(url, { headers: mergeHeaders(buildUpstreamHeaders({ cookie: t.cookie, ref, req })), redirect: 'follow' })
    const text = await r.text()
    const rewritten = rewritePlaylist(text, url, token)
    res.setHeader('Content-Type', 'application/vnd.apple.mpegurl')
    res.setHeader('X-Upstream-Status', String(r.status))
    res.setHeader('X-Upstream-CT', r.headers.get('content-type') || '')
    res.setHeader('X-Upstream-URL', url)
    res.status(r.status).send(rewritten)
  } catch { res.status(500).send('error') }
})
app.get('/proxy/segment', async (req, res) => {
  try {
    const token = String(req.query.token || '')
    const theurl = String(req.query.url || '')
    const ref = String(req.query.ref || '')
    const t = getToken(token)
    if (!t) return res.status(403).send('forbidden')
    const r = await httpGetRaw(theurl, { headers: mergeHeaders(buildUpstreamHeaders({ cookie: t.cookie, ref, req })), redirect: 'follow' })
    pipeFetchResponse(r, res, theurl)
  } catch { res.status(500).end() }
})
app.get('/proxy/key', async (req, res) => {
  try {
    const token = String(req.query.token || '')
    const url = String(req.query.url || '')
    const ref = String(req.query.ref || '')
    const t = getToken(token)
    if (!t) return res.status(403).send('forbidden')
    const r = await httpGetRaw(url, { headers: mergeHeaders(buildUpstreamHeaders({ cookie: t.cookie, ref, req })), redirect: 'follow' })
    pipeFetchResponse(r, res, url)
  } catch { res.status(500).end() }
})

app.get('/favicon.ico', (req, res) => res.status(204).end())
app.get('/health', (req, res) => res.json({ ok: true, time: Date.now() }))

process.on('unhandledRejection', () => {})
process.on('uncaughtException', () => {})

const PORT = 3001
app.listen(PORT, () => { console.log(`listening on :${PORT}`) })
