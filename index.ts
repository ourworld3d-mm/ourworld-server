import { Server, Room, Client, ServerError } from '@colyseus/core'
import { WebSocketTransport } from '@colyseus/ws-transport'
import { createClient, SupabaseClient } from '@supabase/supabase-js'
import { createServer } from 'http'

const SUPABASE_URL = process.env.SUPABASE_URL ?? 'https://hluzmyzggovesxxxwgqi.supabase.co'
const SUPABASE_KEY = process.env.SUPABASE_PUBLISHABLE_KEY ?? ''
const PORT = Number(process.env.PORT ?? 2567)
if (!SUPABASE_KEY) throw new Error('SUPABASE_PUBLISHABLE_KEY is required')

type Pose = { x: number; y: number; z: number; ry: number; anim: string }
type Auth = { userId: string; name: string; db: SupabaseClient; worldId: string }
const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v)

export class WorldRoom extends Room {
  maxClients = 2
  private poses = new Map<string, Pose>()
  private lastChat = new Map<string, number>()

  async onAuth(_client: Client, options: any): Promise<Auth> {
    const token = options?.token, worldId = options?.worldId
    if (typeof token !== 'string' || typeof worldId !== 'string') throw new ServerError(401, 'missing token or worldId')
    if (worldId !== this.metadata?.worldId) throw new ServerError(403, 'wrong world')
    const db = createClient(SUPABASE_URL, SUPABASE_KEY, { global: { headers: { Authorization: `Bearer ${token}` } }, auth: { persistSession: false, autoRefreshToken: false } })
    const { data: u, error } = await db.auth.getUser(token)
    if (error || !u?.user) throw new ServerError(401, 'invalid token')
    // RLS only returns this row if the caller is a member of the world.
    const { data: m, error: e2 } = await db.from('world_members').select('user_id').eq('world_id', worldId).eq('user_id', u.user.id).maybeSingle()
    if (e2 || !m) throw new ServerError(403, 'not a member of this world')
    const { data: p } = await db.from('profiles').select('display_name').eq('id', u.user.id).maybeSingle()
    return { userId: u.user.id, name: p?.display_name ?? 'Player', db, worldId }
  }

  async onCreate(options: any) {
    this.setMetadata({ worldId: options.worldId })
    this.onMessage('pose', (client, d: any) => {
      if (!d || !isNum(d.x) || !isNum(d.y) || !isNum(d.z) || !isNum(d.ry) || typeof d.anim !== 'string') return
      const a = client.auth as Auth
      const pose: Pose = { x: d.x, y: d.y, z: d.z, ry: d.ry, anim: d.anim.slice(0, 24) }
      this.poses.set(a.userId, pose)
      this.broadcast('pose', { id: a.userId, ...pose }, { except: client })
    })
    this.onMessage('chat', async (client, d: any) => {
      const a = client.auth as Auth
      const body = typeof d?.body === 'string' ? d.body.trim().slice(0, 500) : ''
      if (!body) return
      const now = Date.now()
      if (now - (this.lastChat.get(a.userId) ?? 0) < 300) return
      this.lastChat.set(a.userId, now)
      const { error } = await a.db.from('messages').insert({ world_id: a.worldId, user_id: a.userId, body })
      if (error) { client.send('error', { message: 'message rejected' }); return }
      this.broadcast('chat', { id: a.userId, name: a.name, body, ts: now })
    })
  }

  onJoin(client: Client) {
    const a = client.auth as Auth
    const others = [...this.poses.entries()].filter(([id]) => id !== a.userId).map(([id, p]) => ({ id, ...p }))
    client.send('welcome', { id: a.userId, name: a.name, others })
    this.broadcast('peer-joined', { id: a.userId, name: a.name }, { except: client })
  }

  async onLeave(client: Client, consented: boolean) {
    const a = client.auth as Auth
    if (!consented) {
      try { await this.allowReconnection(client, 20); return } catch { /* timed out */ }
    }
    this.poses.delete(a.userId)
    this.broadcast('peer-left', { id: a.userId })
  }
}

const httpServer = createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' }); res.end('ok'); return }
  res.writeHead(404); res.end()
})
const gameServer = new Server({ transport: new WebSocketTransport({ server: httpServer }) })
gameServer.define('world', WorldRoom).filterBy(['worldId'])
httpServer.listen(PORT, () => console.log(`ow3d server listening on :${PORT}`))
export { gameServer }
