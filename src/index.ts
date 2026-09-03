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

// Un paragrafo del documento, con un'indicazione se sembra un titolo di
// capitolo (usato dal frontend per la preview strutturata).
interface DocParagraph {
  text: string
  isHeading: boolean
}

// Valore più frequente in un array di numeri (usato per stimare la
// dimensione del "testo normale" di un documento, contro cui confrontare
// i titoli).
function mode(values: number[]): number {
  if (values.length === 0) return 0
  const counts = new Map<number, number>()
  for (const v of values) counts.set(v, (counts.get(v) ?? 0) + 1)
  let best = values[0]
  let bestCount = 0
  for (const [v, c] of counts) {
    if (c > bestCount) {
      best = v
      bestCount = c
    }
  }
  return best
}

function median(values: number[]): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

// Estrae il PDF pagina per pagina, ricostruendo righe e paragrafi dalla
// posizione e dimensione reale del testo (disponibile nei PDF, a differenza
// del testo semplice): una riga corta, con font più grande del normale e
// preceduta da uno spazio verticale maggiore del solito, viene considerata
// un titolo di capitolo.
async function extractPdfStructuredPages(buffer: Buffer): Promise<DocParagraph[][]> {
  const pages: DocParagraph[][] = []

  await pdfParse(buffer, {
    pagerender: async (pageData: {
      pageNumber: number
      getTextContent: () => Promise<{
        items: Array<{ str: string; transform: number[] }>
      }>
    }) => {
      const content = await pageData.getTextContent()

      // Raggruppa gli item di testo in righe, in base alla coordinata Y
      type Line = { y: number; size: number; text: string }
      const lines: Line[] = []
      for (const item of content.items) {
        const y = item.transform[5]
        const size =
          Math.hypot(item.transform[2], item.transform[3]) ||
          Math.abs(item.transform[3])
        const last = lines[lines.length - 1]
        if (last && Math.abs(last.y - y) < 2) {
          last.text += item.str
          last.size = Math.max(last.size, size)
        } else {
          lines.push({ y, size, text: item.str })
        }
      }

      // Dimensione del "corpo del testo": la più frequente tra le righe
      const bodySize = mode(lines.map((l) => Math.round(l.size)))

      // Spaziatura verticale tipica tra righe consecutive dello stesso
      // paragrafo, per riconoscere un salto più ampio (= nuovo paragrafo)
      const gaps: number[] = []
      for (let i = 1; i < lines.length; i++) {
        gaps.push(Math.abs(lines[i].y - lines[i - 1].y))
      }
      const typicalGap = median(gaps)

      const paragraphs: DocParagraph[] = []
      let buf = ''
      let bufIsHeading = false

      for (let i = 0; i < lines.length; i++) {
        const text = lines[i].text.trim()
        if (!text) continue

        const gapBefore = i > 0 ? Math.abs(lines[i].y - lines[i - 1].y) : 0
        const isNewParagraph = i === 0 || gapBefore > typicalGap * 1.6

        if (isNewParagraph) {
          if (buf.trim()) paragraphs.push({ text: buf.trim(), isHeading: bufIsHeading })
          const wordCount = text.split(/\s+/).length
          bufIsHeading =
            typicalGap > 0 &&
            Math.round(lines[i].size) > bodySize * 1.15 &&
            wordCount <= 12
          buf = text
        } else {
          buf += ' ' + text
        }
      }
      if (buf.trim()) paragraphs.push({ text: buf.trim(), isHeading: bufIsHeading })

      pages[pageData.pageNumber - 1] = paragraphs
      return paragraphs.map((p) => p.text).join('\n\n')
    },
  })

  return pages.map((p) => p ?? [])
}

// Analizza un blocco XML di un documento .docx (o un intero documento) e
// ne estrae i paragrafi, rilevando i titoli tramite lo stile Word
// ("Heading"/"Titolo") o una dimensione del font maggiore del normale.
function parseDocxParagraphs(segmentXml: string): DocParagraph[] {
  const paraMatches = [...segmentXml.matchAll(/<w:p\b[^>]*>([\s\S]*?)<\/w:p>/g)]
  const raw: { text: string; sz: number; styleHeading: boolean }[] = []

  for (const pm of paraMatches) {
    const pXml = pm[1]
    const textMatches = [...pXml.matchAll(/<w:t[^>]*>([^<]*)<\/w:t>/g)]
    const text = textMatches.map((m) => m[1]).join('').trim()
    if (!text) continue

    const styleMatch = pXml.match(/<w:pStyle w:val="([^"]+)"/)
    const styleHeading = !!styleMatch && /heading|title|titolo/i.test(styleMatch[1])

    const szMatches = [...pXml.matchAll(/<w:sz w:val="(\d+)"/g)].map((m) =>
      parseInt(m[1], 10)
    )
    const maxSz = szMatches.length ? Math.max(...szMatches) : 0

    raw.push({ text, sz: maxSz, styleHeading })
  }

  const bodySize = mode(raw.filter((p) => !p.styleHeading && p.sz > 0).map((p) => p.sz)) || 22

  return raw.map((p) => {
    const wordCount = p.text.split(/\s+/).length
    const isHeading = p.styleHeading || (p.sz > bodySize * 1.15 && wordCount <= 12)
    return { text: p.text, isHeading }
  })
}

// I file .docx non contengono numeri di pagina reali (dipendono da come
// verrà stampato/visualizzato), a meno che l'autore non abbia inserito
// interruzioni di pagina esplicite. Qui leggiamo l'XML interno del docx
// (è uno zip) e dividiamo su quelle interruzioni, se presenti; altrimenti
// il documento intero è considerato un'unica "pagina".
async function extractDocxStructuredPages(buffer: Buffer): Promise<DocParagraph[][]> {
  try {
    const zip = await JSZip.loadAsync(buffer)
    const xml = await zip.file('word/document.xml')?.async('string')

    if (xml) {
      const segments = xml.split(/<w:br[^>]*w:type="page"[^>]*\/?>/g)
      const pages = segments.map(parseDocxParagraphs).filter((p) => p.length > 0)
      if (pages.length > 0) return pages
    }
  } catch (err) {
    console.error('Errore lettura struttura docx:', err)
    // Ricade sull'estrazione semplice con mammoth qui sotto
  }

  const result = await mammoth.extractRawText({ buffer })
  const text = normalizeExtractedText(result.value)
  return [text.split('\n\n').filter((p) => p.trim()).map((t) => ({ text: t, isHeading: false }))]
}

// Esegue l'OCR su una o più immagini, una per pagina, nell'ordine in cui
// sono state caricate. L'OCR non fornisce informazioni affidabili sulla
// dimensione del font, quindi qui i titoli vengono riconosciuti solo con
// un'euristica testuale: un paragrafo composto da una singola riga breve,
// senza punteggiatura di fine frase.
async function extractImageStructuredPages(files: File[]): Promise<DocParagraph[][]> {
  const worker = await createWorker(['ita', 'eng'])
  try {
    const pages: DocParagraph[][] = []

    for (const file of files) {
      const buffer = Buffer.from(await file.arrayBuffer())
      const { data } = await worker.recognize(buffer)

      const ocrParagraphs = (
        (data as unknown as {
          paragraphs?: Array<{ text: string; lines?: unknown[] }>
        }).paragraphs ?? []
      )
        .map((p) => {
          const text = normalizeExtractedText(p.text ?? '')
          const wordCount = text.split(/\s+/).filter(Boolean).length
          const lineCount = p.lines?.length ?? 1
          const isHeading =
            lineCount <= 1 && wordCount > 0 && wordCount <= 8 && !/[.,;:]$/.test(text.trim())
          return { text, isHeading }
        })
        .filter((p) => p.text.length > 0)

      if (ocrParagraphs.length > 0) {
        pages.push(ocrParagraphs)
      } else {
        const text = normalizeExtractedText(data.text)
        if (text) pages.push([{ text, isHeading: false }])
      }
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

    let pages: DocParagraph[][]

    if (files.length === 1 && firstName.endsWith('.docx')) {
      pages = await extractDocxStructuredPages(Buffer.from(await files[0].arrayBuffer()))
    } else if (files.length === 1 && firstName.endsWith('.pdf')) {
      pages = await extractPdfStructuredPages(Buffer.from(await files[0].arrayBuffer()))
    } else if (allImages) {
      pages = await extractImageStructuredPages(files)
    } else {
      return c.json(
        {
          error:
            'Formato non supportato o combinazione di file non valida. Usa un singolo .docx, un singolo .pdf, oppure una o più immagini (.jpg, .png).',
        },
        400
      )
    }

    pages = pages
      .map((page) => page.filter((p) => p.text.trim().length > 0))
      .filter((page) => page.length > 0)

    if (pages.length === 0) {
      return c.json(
        { error: 'Non è stato possibile estrarre testo da questo file. Prova con un file più nitido o leggibile.' },
        422
      )
    }

    // Limite di sicurezza per pagina (somma dei caratteri dei paragrafi):
    // l'utente sceglierà poi l'intervallo da usare, ma evitiamo comunque
    // di restituire pagine abnormemente lunghe.
    pages = pages.map((page) => {
      let total = 0
      const capped: DocParagraph[] = []
      for (const p of page) {
        if (total + p.text.length > 50_000) break
        capped.push(p)
        total += p.text.length
      }
      return capped
    })

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