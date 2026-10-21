/**
 * SyncEngine — the app's single data layer, living in the main process.
 * Both windows (widget + Manage) render the same AppState pushed over IPC, so a change in one
 * shows up in the other immediately. Only the server touches the database; we talk REST + WebSocket.
 *
 *  - local cache (cache.json) of the last snapshot → app works offline / starts instantly
 *  - outbox (outbox.json) of queued mutations, applied optimistically, flushed in order
 *  - WebSocket: server broadcasts after each mutation → we re-fetch the snapshot
 *  - last-write-wins: each op carries the local edit time as `updatedAt`; the server keeps the newest
 */
import { EventEmitter } from 'node:events';
import crypto from 'node:crypto';
import WebSocket from 'ws';
import type { ServerEvent, SignInResponse, Snapshot } from '@desklist/shared';
import type { AppState, Connection, Op, QueuedOp } from '../common/ipc';
import { applyOp, entityKey, toRequest } from '../common/sync-ops';
import { getSettings, loadToken, readJson, saveToken, updateSettings, writeJson } from './settings';

export { applyOp };

class NetworkError extends Error {}
export class ApiError extends Error { constructor(public status: number, msg: string) { super(msg); } }


export class SyncEngine extends EventEmitter {
  private token: string | null = loadToken();
  private base: Snapshot | null = validSnap(readJson<Snapshot | null>('cache.json', null));
  private outbox: QueuedOp[] = readJson<QueuedOp[]>('outbox.json', []);
  private connection: Connection = 'connecting';
  private lastSyncedAt: string | null = null;
  private lastError: string | null = null;
  private ws: WebSocket | null = null;
  private backoff = 1000;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private refreshTimer: NodeJS.Timeout | null = null;
  private poll: NodeJS.Timeout | null = null;
  private flushing = false;
  private inFlight: string | null = null;

  start() {
    if (this.token) this.connect();
    this.poll = setInterval(() => { if (this.token) this.refresh().catch(() => {}); }, 60_000);
    this.emitState();
  }
  stop() { if (this.poll) clearInterval(this.poll); this.ws?.close(); }

  get serverUrl() { return getSettings().serverUrl.replace(/\/+$/, ''); }

  state(): AppState {
    let snap = this.base;
    if (snap) for (const q of this.outbox) snap = applyOp(snap, q.op, snap.me, q.at);
    const s = getSettings();
    return {
      phase: !this.token ? 'signedOut' : snap ? 'ready' : 'loading',
      connection: this.connection, snapshot: snap, lastSyncedAt: this.lastSyncedAt, pending: this.outbox.length,
      lastError: this.lastError, serverUrl: this.serverUrl, platform: process.platform,
      settings: { lockPosition: s.lockPosition, launchAtLogin: s.launchAtLogin, currentProjectId: s.currentProjectId ?? null, sessionExpiresAt: s.sessionExpiresAt ?? null, widgetOpacity: s.widgetOpacity ?? 85, widgetBlur: s.widgetBlur ?? true, autoShowWidget: s.autoShowWidget ?? true, openManageAtStartup: s.openManageAtStartup ?? false, startMinimized: s.startMinimized ?? false },
    };
  }
  private emitState() { this.emit('state', this.state()); }

  async http<T>(method: string, path: string, body?: unknown, token = this.token): Promise<T> {
    let r: Response;
    try {
      r = await fetch(this.serverUrl + path, {
        method, headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
        body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10_000),
      });
    } catch (e) {
      const timedOut = (e as Error)?.name === 'TimeoutError' || (e as Error)?.name === 'AbortError';
      throw new NetworkError(timedOut ? `The Desklist server at ${this.serverUrl} did not respond within 10 seconds. Check your internet connection or the server URL, then try again.`
        : `Cannot reach the Desklist server at ${this.serverUrl}. Check your internet connection or the server URL.`);
    }
    const json: any = await r.json().catch(() => ({}));
    if (!r.ok && json.error == null && (r.status === 404 || r.status >= 500))
      throw new ApiError(r.status, `The Desklist server at ${this.serverUrl} is not available right now (HTTP ${r.status}). Try again in a minute or check the server URL.`);
    if (r.status === 401 && token && path !== '/api/auth/signin') { this.signOutLocal('Session expired — please sign in again'); }
    if (!r.ok) throw new ApiError(r.status, json.error ?? `HTTP ${r.status}`);
    return json as T;
  }

  /** Upload raw bytes as a task file (stored in Postgres bytea on the server). */
  async upload(taskId: string, name: string, mime: string, data: Uint8Array, fieldId?: string | null): Promise<unknown> {
    const r = await fetch(this.serverUrl + `/api/tasks/${taskId}/files`, { method: 'POST', body: Buffer.from(data), signal: AbortSignal.timeout(60_000),
      headers: { 'content-type': 'application/octet-stream', 'x-filename': encodeURIComponent(name), 'x-mime': mime || 'application/octet-stream',
        ...(fieldId ? { 'x-field-id': fieldId } : {}), ...(this.token ? { authorization: `Bearer ${this.token}` } : {}) } });
    const body = await r.text();
    if (!r.ok) { let m = `HTTP ${r.status}`; try { m = JSON.parse(body).error ?? m; } catch { /* */ } throw new ApiError(r.status, m); }
    return JSON.parse(body);
  }
  async download(path: string): Promise<Buffer> {
    const r = await fetch(this.serverUrl + path, { headers: this.token ? { authorization: `Bearer ${this.token}` } : {}, signal: AbortSignal.timeout(60_000) });
    if (!r.ok) throw new ApiError(r.status, `HTTP ${r.status}`);
    return Buffer.from(await r.arrayBuffer());
  }
  /** GET a non-JSON resource (CSV export). */
  async text(path: string): Promise<string> {
    const r = await fetch(this.serverUrl + path, { headers: this.token ? { authorization: `Bearer ${this.token}` } : {}, signal: AbortSignal.timeout(20_000) });
    const body = await r.text();
    if (!r.ok) { let m = `HTTP ${r.status}`; try { m = JSON.parse(body).error ?? m; } catch { /* */ } throw new ApiError(r.status, m); }
    return body;
  }

  async signIn(code: string, serverUrl?: string) {
    if (serverUrl) updateSettings({ serverUrl: serverUrl.replace(/\/+$/, '') });
    const res = await this.http<SignInResponse>('POST', '/api/auth/signin', { code }, null);
    await this.adoptSession(res);
  }
  get baseUrl() { return this.serverUrl; }
  /** 0.6.4 username + password accounts. Returns pending (needs admin code) or signs in. */
  async account(kind: 'signup' | 'login' | 'redeem', body: Record<string, string>, serverUrl?: string): Promise<{ pending: boolean; accountToken?: string; username?: string }> {
    if (serverUrl) updateSettings({ serverUrl: serverUrl.replace(/\/+$/, '') });
    const res = await this.http<any>('POST', `/api/accounts/${kind}`, body, null);
    if (res.pending) return { pending: true, accountToken: res.accountToken, username: res.username };
    await this.adoptSession(res); return { pending: false };
  }
  /** Passkey: exchange the one-time code (+ PKCE verifier) from the loopback redirect for a session. */
  async signInWithPasskeyCode(code: string, verifier: string) {
    const res = await this.http<SignInResponse>('POST', '/api/auth/passkey/exchange', { code, verifier }, null);
    await this.adoptSession(res);
  }
  async signInWithGoogle(body: { code: string; verifier: string; redirectUri: string; state: string }) {
    const res = await this.http<SignInResponse>('POST', '/api/auth/google/exchange', body, null);
    await this.adoptSession(res);
  }
  /** Unauthenticated call (sign-in screen). */
  anon<T>(method: string, path: string, body?: unknown) { return this.http<T>(method, path, body, null); }
  private async adoptSession(res: SignInResponse) {
    if (this.base && this.base.me?.id !== res.member?.id) { this.base = null; this.outbox = []; this.persist(); }
    this.token = res.token; saveToken(res.token); if ((res as any).expiresAt) updateSettings({ sessionExpiresAt: (res as any).expiresAt });
    this.lastError = null;
    try {
      // load the first snapshot before reporting success, so the UI never sits on "Loading…"
      this.base = await this.http<Snapshot>('GET', '/api/snapshot');
      this.lastSyncedAt = new Date().toISOString(); writeJson('cache.json', this.base);
    } catch (e) {
      this.signOutLocal(null);
      throw e;
    }
    this.connect();
    await this.refresh().catch(() => {});
  }
  async signOut() {
    if (this.token) await this.http('POST', '/api/auth/signout').catch(() => {});
    this.signOutLocal(null);
  }
  private signOutLocal(msg: string | null) {
    this.token = null; saveToken(null);
    this.base = null; this.outbox = []; this.persist();
    this.ws?.removeAllListeners(); this.ws?.close(); this.ws = null;
    this.lastError = msg; this.connection = 'offline';
    this.emitState();
  }

  /** Optimistic mutation: apply locally now, queue, flush. */
  mutate(op: Op) {
    const at = new Date().toISOString();
    const key = entityKey(op);
    const last = this.outbox[this.outbox.length - 1];
    // coalesce rapid edits (e.g. notes autosave) into the last queued op for the same entity
    if (last && key && entityKey(last.op) === key && last.opId !== this.inFlight && (op.kind === 'project.update' || op.kind === 'task.update' || op.kind === 'column.update')) {
      const prev = last.op as any; const values = { ...(prev.patch.values ?? {}), ...((op as any).patch.values ?? {}) };
      last.op = { ...op, patch: { ...prev.patch, ...(op as any).patch, ...(Object.keys(values).length ? { values } : {}) } } as Op;
      last.at = at;
    } else {
      this.outbox.push({ opId: crypto.randomUUID(), at, op });
    }
    this.persist(); this.emitState();
    void this.flush();
  }

  /** Wait (bounded) until queued optimistic ops reached the server, so online-only requests see e.g. a just-created task. */
  async drain(timeoutMs = 8000) {
    const end = Date.now() + timeoutMs;
    void this.flush();
    while (this.outbox.length && Date.now() < end) await new Promise((r) => setTimeout(r, 100));
  }

  async flush() {
    if (this.flushing || !this.token) return;
    this.flushing = true;
    try {
      while (this.outbox.length) {
        const q = this.outbox[0]; const req = toRequest(q);
        this.inFlight = q.opId;
        try {
          await this.http(req.method, req.path, req.body);
        } catch (e) {
          if (e instanceof NetworkError || (e instanceof ApiError && e.status >= 500) || (e instanceof ApiError && e.status === 429)) {
            this.setConnection('offline'); return; // keep op, retry on reconnect
          }
          if (e instanceof ApiError && e.status === 401) return;
          // 4xx: the server rejected it — drop it (the snapshot refresh reverts the optimistic change)
          if (!(e instanceof ApiError && e.status === 404 && q.op.kind.endsWith('.delete'))) this.lastError = (e as Error).message;
        } finally { this.inFlight = null; }
        this.outbox.shift(); this.persist();
      }
    } finally { this.flushing = false; }
    this.scheduleRefresh(0);
  }

  async refresh() {
    if (!this.token) return;
    try {
      const snap = await this.http<Snapshot>('GET', '/api/snapshot');
      this.base = snap; this.lastSyncedAt = new Date().toISOString();
      if (this.connection !== 'online' && this.ws?.readyState === WebSocket.OPEN) this.connection = 'online';
      writeJson('cache.json', snap);
      const s = getSettings();
      if (!s.currentProjectId || !snap.projects.some((p) => p.id === s.currentProjectId)) updateSettings({ currentProjectId: snap.projects[0]?.id });
    } catch (e) {
      if (e instanceof NetworkError) this.setConnection('offline');
      else throw e;
    } finally { this.emitState(); }
  }
  private scheduleRefresh(ms = 150) {
    if (this.refreshTimer) clearTimeout(this.refreshTimer);
    this.refreshTimer = setTimeout(() => { this.refresh().catch((e) => { this.lastError = e.message; this.emitState(); }); }, ms);
  }
  private setConnection(c: Connection) { if (this.connection !== c) { this.connection = c; this.emitState(); } }

  private connect() {
    if (!this.token) return;
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    this.ws?.removeAllListeners(); this.ws?.close();
    this.setConnection('connecting');
    const url = this.serverUrl.replace(/^http/, 'ws') + '/ws?token=' + encodeURIComponent(this.token);
    const ws = new WebSocket(url); this.ws = ws;
    ws.on('open', () => {
      this.backoff = 1000; this.setConnection('online');
      void this.flush().then(() => this.scheduleRefresh(0)); // sync on reconnect
    });
    ws.on('message', (raw) => {
      try {
        const ev = JSON.parse(String(raw)) as ServerEvent;
        if (ev.type === 'changed') this.scheduleRefresh();
      } catch { /* ignore */ }
    });
    const retry = () => {
      if (this.ws !== ws || !this.token) return;
      this.setConnection('offline');
      this.reconnectTimer = setTimeout(() => this.connect(), this.backoff);
      this.backoff = Math.min(this.backoff * 2, 30_000);
    };
    ws.on('close', retry);
    ws.on('error', () => { /* close follows */ });
  }
  /** for a "Sync now" button / server URL change */
  reconnect() { this.backoff = 1000; this.connect(); }

  private persist() { writeJson('outbox.json', this.outbox); if (this.base) writeJson('cache.json', this.base); }
  clearError() { this.lastError = null; this.emitState(); }
}

/** 0.6.17: a stale/partial cache.json (no `me`) must never break sign-in. */
function validSnap(s: any): Snapshot | null { return s && typeof s === 'object' && s.me?.id && Array.isArray(s.projects) ? s : null; }
