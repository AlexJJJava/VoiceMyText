import { Hono } from 'hono'
import { serve } from '@hono/node-server'
import { cors } from 'hono/cors'
import { Communicate } from 'edge-tts-universal'
import { createClient } from '@supabase/supabase-js'
import { randomUUID } from 'node:crypto'
import mammoth from 'mammoth'
import pdfParse from 'pdf-parse'
import { createWorker } from 'tesseract.js'
import JSZip from 'jszip'

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
// Adatto a un singolo server; se in futuro si scala su più istanze,
// va sostituito con uno store condiviso (es. Redis).
function createRateLimiter(max: number, windowMs: number) {
  const log = new Map<string, number[]>()

  return function isLimited(ip: string): boolean {
    const now = Date.now()
    const timestamps = log.get(ip) ?? []

    // Tiene solo le richieste dentro la finestra temporale corrente
    const recent = timestamps.filter((t) => now - t < windowMs)

    if (recent.length >= max) {
      log.set(ip, recent)
      return true
    }

    recent.push(now)
    log.set(ip, recent)
    return false
  }
}

const RATE_LIMIT_MAX = 5
const RATE_LIMIT_WINDOW_MS = 60_000
const isTtsRateLimited = createRateLimiter(RATE_LIMIT_MAX, RATE_LIMIT_WINDOW_MS)
// Limiter separato per l'estrazione testo (più pesante: OCR/parsing file),
// così non condivide il budget di richieste con la generazione audio.
const isExtractRateLimited = createRateLimiter(5, RATE_LIMIT_WINDOW_MS)

function getClientIp(c: { req: { header: (name: string) => string | undefined } }): string {
  return (
    c.req.header('x-forwarded-for')?.split(',')[0].trim() ??
    c.req.header('x-real-ip') ??
    'unknown'
  )
}

app.post('/tts', async (c) => {
  const ip = getClientIp(c)

  if (isTtsRateLimited(ip)) {
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
// ── Estrazione testo da file (.docx, .pdf, immagini via OCR) ───────
// Non richiede autenticazione: è un passaggio preliminare alla
// generazione audio, non tocca dati dell'utente. Protetto comunque da
// rate limiting perché più pesante di una normale richiesta.
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024 // 25 MB (più immagini insieme come pagine)

// Il testo estratto da PDF e OCR mantiene gli "a-capo" originali del
// documento (dettati dall'impaginazione, non dalla fine della frase).
// Se li lasciamo, il motore TTS li legge come pause innaturali a metà
// frase. Qui uniamo le righe che appartengono allo stesso paragrafo,
// mantenendo solo le vere interruzioni (paragrafi separati da riga vuota).
function normalizeExtractedText(raw: string): string {
  const paragraphs = raw.split(/\n\s*\n+/)
  return paragraphs
    .map((p) => p.replace(/\s*\n\s*/g, ' ').replace(/[ \t]+/g, ' ').trim())
    .filter((p) => p.length > 0)
    .join('\n\n')
}

// Estrae il testo di un PDF pagina per pagina, usando l'hook "pagerender"
// di pdf-parse per intercettare il contenuto di ogni singola pagina invece
// di ricevere solo il testo dell'intero documento concatenato.
async function extractPdfPages(buffer: Buffer): Promise<string[]> {
  const pageTexts: string[] = []

  await pdfParse(buffer, {
    pagerender: async (pageData: {
      pageNumber: number
      getTextContent: () => Promise<{ items: Array<{ str: string }> }>
    }) => {
      const content = await pageData.getTextContent()
      const text = content.items.map((item) => item.str).join(' ')
      pageTexts[pageData.pageNumber - 1] = text
      return text
    },
  })

  return pageTexts.map((t) => normalizeExtractedText(t ?? ''))
}

// I file .docx non contengono numeri di pagina reali (dipendono da come
// verrà stampato/visualizzato), a meno che l'autore non abbia inserito
// interruzioni di pagina esplicite. Qui leggiamo l'XML interno del docx
// (è uno zip) e dividiamo il testo su quelle interruzioni, se presenti.
// Se non ce ne sono, restituiamo l'intero documento come unica "pagina".
async function extractDocxPages(buffer: Buffer): Promise<string[]> {
  try {
    const zip = await JSZip.loadAsync(buffer)
    const xml = await zip.file('word/document.xml')?.async('string')

    if (xml) {
      const segments = xml.split(/<w:br[^>]*w:type="page"[^>]*\/?>/g)
      if (segments.length > 1) {
        const pages = segments
          .map((segment) => {
            const matches = [...segment.matchAll(/<w:t[^>]*>([^<]*)<\/w:t>/g)]
            const rawText = matches.map((m) => m[1]).join(' ')
            return normalizeExtractedText(rawText)
          })
          .filter((p) => p.length > 0)
        if (pages.length > 1) return pages
      }
    }
  } catch (err) {
    console.error('Errore lettura interruzioni di pagina docx:', err)
    // Ricade sull'estrazione semplice con mammoth qui sotto
  }

  // Nessuna interruzione di pagina esplicita trovata: documento intero
  // come unica pagina, tramite l'estrazione standard (più affidabile).
  const result = await mammoth.extractRawText({ buffer })
  return [normalizeExtractedText(result.value)]
}

// Esegue l'OCR su una o più immagini, una per pagina, nell'ordine in cui
// sono state caricate.
async function extractImagePages(files: File[]): Promise<string[]> {
  const worker = await createWorker(['ita', 'eng'])
  try {
    const pages: string[] = []
    for (const file of files) {
      const buffer = Buffer.from(await file.arrayBuffer())
      const { data } = await worker.recognize(buffer)
      pages.push(normalizeExtractedText(data.text))
    }
    return pages
  } finally {
    await worker.terminate()
  }
}

app.post('/extract-text', async (c) => {
  const ip = getClientIp(c)

  if (isExtractRateLimited(ip)) {
    return c.json(
      { error: 'Troppe richieste. Massimo 5 al minuto, riprova tra poco.' },
      429
    )
  }

  const contentLengthHeader = c.req.header('content-length')
  const contentLength = contentLengthHeader ? parseInt(contentLengthHeader, 10) : 0
  if (contentLength > MAX_UPLOAD_BYTES) {
    return c.json({ error: 'File troppo grande (massimo 25MB in totale)' }, 413)
  }

  try {
    const body = await c.req.parseBody({ all: true })
    const rawFiles = body.files

    const files: File[] = Array.isArray(rawFiles)
      ? rawFiles.filter((f): f is File => f instanceof File)
      : rawFiles instanceof File
      ? [rawFiles]
      : []

    if (files.length === 0) {
      return c.json({ error: 'File mancante' }, 400)
    }

    const firstName = files[0].name.toLowerCase()
    const allImages = files.every((f) => /\.(jpe?g|png)$/.test(f.name.toLowerCase()))

    let pages: string[]

    if (files.length === 1 && firstName.endsWith('.docx')) {
      pages = await extractDocxPages(Buffer.from(await files[0].arrayBuffer()))
    } else if (files.length === 1 && firstName.endsWith('.pdf')) {
      pages = await extractPdfPages(Buffer.from(await files[0].arrayBuffer()))
    } else if (allImages) {
      pages = await extractImagePages(files)
    } else {
      return c.json(
        {
          error:
            'Formato non supportato o combinazione di file non valida. Usa un singolo .docx, un singolo .pdf, oppure una o più immagini (.jpg, .png).',
        },
        400
      )
    }

    pages = pages.filter((p) => p.trim().length > 0)

    if (pages.length === 0) {
      return c.json(
        { error: 'Non è stato possibile estrarre testo da questo file. Prova con un file più nitido o leggibile.' },
        422
      )
    }

    // Limite di sicurezza per singola pagina (l'utente sceglierà poi
    // l'intervallo da usare, ma evitiamo comunque pagine abnormi)
    pages = pages.map((p) => (p.length > 50_000 ? p.slice(0, 50_000) : p))

    // Suggerisce un titolo a partire dal nome del primo file (senza estensione)
    const suggestedTitle = files[0].name.replace(/\.[^/.]+$/, '')

    return c.json({ pages, title: suggestedTitle })
  } catch (err) {
    console.error('Errore imprevisto in /extract-text:', err)
    return c.json({ error: 'Errore durante l\'estrazione del testo dal file' }, 500)
  }
})

app.post('/library', async (c) => {
  const authHeader = c.req.header('authorization') ?? c.req.header('Authorization')
  const accessToken = authHeader?.replace(/^Bearer\s+/i, '')

  if (!accessToken) {
    return c.json({ error: 'Autenticazione richiesta' }, 401)
  }

  const supabase = getUserSupabaseClient(accessToken)

  try {
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

    const body = await c.req.parseBody()

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
  } catch (err) {
    // Rete di sicurezza: qualunque errore imprevisto viene loggato per
    // intero (visibile nei log di Render) e restituito come JSON valido,
    // invece di lasciar "trapelare" una risposta non gestita al client.
    console.error('Errore imprevisto in /library:', err)
    return c.json({ error: 'Errore interno del server' }, 500)
  }
})

const port = Number(process.env.PORT) || 3000
console.log(`🚀 Server avviato su http://localhost:${port}`)
serve({ fetch: app.fetch, port })