import { Hono } from 'hono'
import { serve } from '@hono/node-server'
import { cors } from 'hono/cors'
import { Communicate } from 'edge-tts-universal'

const app = new Hono()

// Se ALLOWED_ORIGIN è impostata (es. in produzione), CORS viene ristretto
// a quel dominio. Senza impostarla (es. in sviluppo locale) resta aperta a '*'.
const allowedOrigin = process.env.ALLOWED_ORIGIN || '*'

app.use('*', cors({ origin: allowedOrigin }))

const VOICES: Record<string, string> = {
  isabella: 'it-IT-IsabellaNeural',
  diego:    'it-IT-DiegoNeural',
  elsa:     'it-IT-ElsaNeural',
}

app.get('/health', (c) => c.json({ status: 'ok' }))
app.get('/voices', (c) => c.json({ voices: Object.keys(VOICES) }))

// ── Rate limiting semplice, in memoria, per IP ─────────────────────
// Limite: 5 richieste al minuto per IP sull'endpoint /tts.
// Adatto a un singolo server; se in futuro si scala su più istanze,
// va sostituito con uno store condiviso (es. Redis).
const RATE_LIMIT_MAX = 5
const RATE_LIMIT_WINDOW_MS = 60_000

const requestLog = new Map<string, number[]>()

function isRateLimited(ip: string): boolean {
  const now = Date.now()
  const timestamps = requestLog.get(ip) ?? []

  // Tiene solo le richieste dentro la finestra temporale corrente
  const recent = timestamps.filter((t) => now - t < RATE_LIMIT_WINDOW_MS)

  if (recent.length >= RATE_LIMIT_MAX) {
    requestLog.set(ip, recent)
    return true
  }

  recent.push(now)
  requestLog.set(ip, recent)
  return false
}

app.post('/tts', async (c) => {
  const ip =
    c.req.header('x-forwarded-for')?.split(',')[0].trim() ??
    c.req.header('x-real-ip') ??
    'unknown'

  if (isRateLimited(ip)) {
    return c.json(
      { error: `Troppe richieste. Massimo ${RATE_LIMIT_MAX} al minuto, riprova tra poco.` },
      429
    )
  }

  let body: { text?: string; voice?: string; rate?: string; pitch?: string }

  try {
    body = await c.req.json()
  } catch {
    return c.json({ error: 'Body JSON non valido' }, 400)
  }

  const text  = body.text?.trim()
  const voice = body.voice ?? 'isabella'
  const rate  = body.rate  ?? '+0%'
  const pitch = body.pitch ?? '+0Hz'

  if (!text) return c.json({ error: 'Testo obbligatorio' }, 400)
  if (text.length > 50_000) return c.json({ error: 'Testo troppo lungo' }, 400)

  // Formato atteso: es. "+20%", "-50%" (rate) e "+10Hz", "-10Hz" (pitch)
  const RATE_PATTERN = /^[+-]\d{1,3}%$/
  const PITCH_PATTERN = /^[+-]\d{1,3}Hz$/

  if (!RATE_PATTERN.test(rate)) {
    return c.json({ error: `Formato "rate" non valido: "${rate}". Atteso es. "+20%" o "-10%".` }, 400)
  }
  if (!PITCH_PATTERN.test(pitch)) {
    return c.json({ error: `Formato "pitch" non valido: "${pitch}". Atteso es. "+10Hz" o "-5Hz".` }, 400)
  }

  const voiceId = VOICES[voice]
  if (!voiceId) {
    return c.json(
      { error: `Voce non valida: "${voice}". Voci disponibili: ${Object.keys(VOICES).join(', ')}` },
      400
    )
  }

  try {
    const communicate = new Communicate(text, { voice: voiceId, rate, pitch })
    const chunks: Buffer[] = []

    for await (const chunk of communicate.stream()) {
      if (chunk.type === 'audio' && chunk.data) {
        chunks.push(Buffer.from(chunk.data))
      }
    }

    if (chunks.length === 0) return c.json({ error: 'Nessun audio generato' }, 500)

    const audioBuffer = Buffer.concat(chunks)
 
    return new Response(audioBuffer, {
      status: 200,
      headers: {
        'Content-Type': 'audio/mpeg',
        'Content-Length': String(audioBuffer.length),
        'Cache-Control': 'no-cache',
        'Access-Control-Allow-Origin': '*',
      },
    })
  } catch (err) {
    console.error('Errore TTS:', err)
    return c.json({ error: 'Errore generazione audio' }, 500)
  }
})

const port = Number(process.env.PORT) || 3000
console.log(`🚀 Server avviato su http://localhost:${port}`)
serve({ fetch: app.fetch, port })