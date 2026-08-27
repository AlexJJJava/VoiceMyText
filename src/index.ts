import { Hono } from 'hono'
import { serve } from '@hono/node-server'
import { cors } from 'hono/cors'
import { Communicate } from 'edge-tts-universal'
import { createClient } from '@supabase/supabase-js'
import { randomUUID } from 'node:crypto'

const app = new Hono()

// Se ALLOWED_ORIGIN è impostata (es. in produzione), CORS viene ristretto
// a quel dominio. Senza impostarla (es. in sviluppo locale) resta aperta a '*'.
const allowedOrigin = process.env.ALLOWED_ORIGIN || '*'

app.use(
  '*',
  cors({
    origin: allowedOrigin,
    allowHeaders: ['Content-Type', 'Authorization'],
  })
)

// La anon key è pensata per essere pubblica: la sicurezza è garantita dalle
// Row Level Security policy di Supabase, non dalla segretezza di questa chiave.
const SUPABASE_URL = 'https://fmgdismlokadyzdzambh.supabase.co'
const SUPABASE_ANON_KEY =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZtZ2Rpc21sb2thZHl6ZHphbWJoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODc3Mjk5OTgsImV4cCI6MjEwMzMwNTk5OH0.bCzCUPAkfQlEZcbn_dQ8ZJBwvcvZ8bC9N7jl_GKYWfI'

const MAX_LIBRARY_ITEMS = 2

// Crea un client Supabase "impersonato" con il token dell'utente che ha
// fatto la richiesta: tutte le operazioni rispettano le Row Level Security
// policy configurate nel database (ogni utente vede/modifica solo i propri
// dati), senza bisogno di usare la service_role key sul backend.
function getUserSupabaseClient(accessToken: string) {
  return createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    global: { headers: { Authorization: `Bearer ${accessToken}` } },
    auth: { persistSession: false },
  })
}

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

// ── Salvataggio in libreria (max 2 audiolibri per utente) ──────────
// Riceve l'audio già generato (dall'anteprima /tts) insieme ai metadati,
// verifica il limite e lo carica su Supabase Storage + database.
app.post('/library', async (c) => {
  const authHeader = c.req.header('authorization') ?? c.req.header('Authorization')
  const accessToken = authHeader?.replace(/^Bearer\s+/i, '')

  if (!accessToken) {
    return c.json({ error: 'Autenticazione richiesta' }, 401)
  }

  const supabase = getUserSupabaseClient(accessToken)

  const { data: userData, error: userError } = await supabase.auth.getUser()
  if (userError || !userData?.user) {
    return c.json({ error: 'Sessione non valida, effettua di nuovo il login' }, 401)
  }
  const userId = userData.user.id

  // Conta gli audiolibri già salvati da questo utente (RLS filtra
  // automaticamente solo le sue righe)
  const { count, error: countError } = await supabase
    .from('audiobooks')
    .select('id', { count: 'exact', head: true })

  if (countError) {
    console.error('Errore conteggio libreria:', countError)
    return c.json({ error: 'Errore nel controllo della libreria' }, 500)
  }

  if ((count ?? 0) >= MAX_LIBRARY_ITEMS) {
    return c.json(
      { error: `Limite di ${MAX_LIBRARY_ITEMS} audiolibri raggiunto. Elimina un audiolibro per salvarne uno nuovo.` },
      403
    )
  }

  let body: Record<string, unknown>
  try {
    body = await c.req.parseBody()
  } catch {
    return c.json({ error: 'Richiesta non valida' }, 400)
  }

  const title = typeof body.title === 'string' ? body.title.trim() : ''
  const originalText = typeof body.original_text === 'string' ? body.original_text.trim() : ''
  const voice = typeof body.voice === 'string' ? body.voice : ''
  const audioFile = body.audio

  if (!title) return c.json({ error: 'Titolo obbligatorio' }, 400)
  if (title.length > 200) return c.json({ error: 'Titolo troppo lungo' }, 400)
  if (!originalText) return c.json({ error: 'Testo mancante' }, 400)
  if (!VOICES[voice]) return c.json({ error: 'Voce non valida' }, 400)
  if (!(audioFile instanceof File)) {
    return c.json({ error: 'File audio mancante' }, 400)
  }

  const audioBuffer = Buffer.from(await audioFile.arrayBuffer())
  const filePath = `${userId}/${randomUUID()}.mp3`

  const { error: uploadError } = await supabase.storage
    .from('audiobooks')
    .upload(filePath, audioBuffer, { contentType: 'audio/mpeg' })

  if (uploadError) {
    console.error('Errore upload storage:', uploadError)
    return c.json({ error: 'Errore nel salvataggio del file audio' }, 500)
  }

  const { data: inserted, error: insertError } = await supabase
    .from('audiobooks')
    .insert({
      user_id: userId,
      title,
      original_text: originalText,
      voice,
      audio_path: filePath,
    })
    .select()
    .single()

  if (insertError) {
    console.error('Errore inserimento libreria:', insertError)
    // Ripulisce il file caricato se l'inserimento nel database fallisce
    await supabase.storage.from('audiobooks').remove([filePath])
    return c.json({ error: 'Errore nel salvataggio in libreria' }, 500)
  }

  return c.json({ audiobook: inserted }, 201)
})


const port = Number(process.env.PORT) || 3000
console.log(`🚀 Server avviato su http://localhost:${port}`)
serve({ fetch: app.fetch, port })