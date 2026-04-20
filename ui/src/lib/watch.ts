import { decodeEntry } from "./kv"
import type { RawEntry, Entry } from "./kv"
  

const WATCH_SUBPROTOCOL = "ws_watch_protocol"

type ClientMessageKind =
  | "WATCH_CREATE"
  | "WATCH_CANCEL"
  | "SESSION_CLOSE" 

type ServerMessageKind =
  | "WATCH_CREATED"   // ServerWatchCreated
  | "WATCH_CANCELED"  // ServerWatchCanceled
  | "PUT"             // ServerWatchEventPut  (= kv.EventPut)
  | "DELETE"          // ServerWatchEventDelete (= kv.EventDelete)
  | "SESSION_CLOSE"   // ServerCloseSession
  | "ERROR"           // ServerError

interface ClientMessage {
  kind: ClientMessageKind
  payload: unknown
}

interface ServerMessage {
  kind: ServerMessageKind
  payload: unknown
}

interface WatchCreateRequest {
  id?: number
  key: string          // base64
  end?: string         // base64, omitted for single-key watch
  prefix?: boolean
  start_revision?: number
  prev_entry?: boolean
  filter?: EventFilter
}

interface WatchCancelRequest {
  watch_id: number
}

// mirrors api.watchResponse (shared by WatchCreateResponse and WatchCancelResponse) json tags: watch_id, success, error
interface WatchResponse {
  watch_id: number
  success: boolean
  error?: string
}

interface RawStreamEvent {
  watcher_id: number   // Wid int64 `json:"watcher_id"`
  event: RawKvEvent
}

interface RawKvEvent {
  kind: "PUT" | "DELETE"   // kv.EventKind
  entry: RawEntry
  prev_entry?: RawEntry
}


interface serverErrorPayload {
  cause?: string
  message?: string
}

export type EventKind   = "PUT" | "DELETE"
export type EventFilter = "NO_PUT" | "NO_DELETE"

export interface WatchEvent {
  watcherId: number
  kind: EventKind
  entry: Entry
  prevEntry?: Entry
}

export interface WatchCreateResult {
  watchId: number
  success: boolean
  error?: string
}

// mirrors api.WatchCreateRequest fields, with plain strings instead of []byte.
export interface WatchCreateOptions {
  key: string
  end?: string           // range end —> creates a [key, end) watch
  prefix?: boolean       // if true, watch all keys with this prefix; overrides end
  startRevision?: number // 0 = current; > 0 = catch up from that revision
  prevEntry?: boolean    // request prev_entry on each event
  filter?: EventFilter   // filter out PUT or DELETE events
}

// Options for constructing the WatchClient.
export interface WatchClientOptions {
  onError?: (message: string) => void   // called on unattributed server errors
  onClose?: () => void                  // called when the WebSocket closes
}

export type WatchEventHandler = (event: WatchEvent) => void

//   const wc = new WatchClient("ws://kave.example.com/v1/watch")
//   await wc.ready()
//   const { watchId } = await wc.watch({ key: "foo" }, ev => console.log(ev))
//   wc.cancel(watchId)
//   wc.close()
export class WatchClient {
  private readonly ws: WebSocket

  private readonly connected: Promise<void>

  private readonly handlers = new Map<number, WatchEventHandler>()

  // FIFO q for inflight WATCH_CREATE requests.
  // resolve/reject + handler, so we can register it synchronously in dispatch().
  private pendingCreates: Array<{
    resolve: (result: WatchCreateResult) => void
    reject: (err: Error) => void
    handler: WatchEventHandler
  }> = []

  private readonly onErrorCallback?: (message: string) => void
  private readonly onCloseCallback?: () => void

  constructor(url: string, opts: WatchClientOptions = {}) {
    this.onErrorCallback = opts.onError
    this.onCloseCallback = opts.onClose

    this.ws = new WebSocket(url, WATCH_SUBPROTOCOL)

    this.connected = new Promise((resolve, reject) => {
      this.ws.onopen  = () => resolve()
      this.ws.onerror = () => reject(new Error("WatchClient: WebSocket connection failed"))
    })

    this.ws.onmessage = (e: MessageEvent<string>) => {
      let msg: ServerMessage
      try {
        msg = JSON.parse(e.data) as ServerMessage
      } catch {
        this.onErrorCallback?.("WatchClient: failed to parse server message")
        return
      }
      this.dispatch(msg)
    }

    this.ws.onclose = () => {
      for (const p of this.pendingCreates) {
        p.reject(new Error("WatchClient: connection closed before watch was created"))
      }
      this.pendingCreates = []
      this.onCloseCallback?.()
    }
  }

  // esolves when the WebSocket handshake is complete.
  ready(): Promise<void> {
    return this.connected
  }

  // returns a promise that resolves with the
  // WatchCreateResult once the server sends back WATCH_CREATED.
  // The handler is registered synchronously when WATCH_CREATED arrives,
  // so no events are missed between creation and handler setup.
  async watch(opts: WatchCreateOptions, handler: WatchEventHandler): Promise<WatchCreateResult> {
    await this.ready()

    return new Promise((resolve, reject) => {
      this.pendingCreates.push({ resolve, reject, handler })
      this.send("WATCH_CREATE", this.encodeCreateRequest(opts))
    })
  }

  // fire and forget: the handler is removed immediately
  // so no more events will be delivered even before the server acknowledges.
  // The server will send WATCH_CANCELED which we silently acknowledge.
  cancel(watchId: number): void {
    this.handlers.delete(watchId)
    const req: WatchCancelRequest = { watch_id: watchId }
    this.send("WATCH_CANCEL", req)
  }

  // Sends SESSION_CLOSE and closes the WebSocket.
  // All active watchers are implicitly dropped on the server side.
  close(): void {
    this.send("SESSION_CLOSE", {})
    this.ws.close()
  }

  get readyState(): number {
    return this.ws.readyState
  }

  // routing incoming server messages
  private dispatch(msg: ServerMessage): void {
    switch (msg.kind) {
      case "WATCH_CREATED": {
        const res = msg.payload as WatchResponse
        const pending = this.pendingCreates.shift()
        if (!pending) return

        if (res.success) {
          // Register the handler synchronously, before resolving the Promise.
          // This prevents a race where the server sends the first event
          // between WATCH_CREATED and when the caller's .then() runs.
          this.handlers.set(res.watch_id, pending.handler)
          pending.resolve({ watchId: res.watch_id, success: true })
        } else {
          pending.resolve({
            watchId: res.watch_id,
            success: false,
            error: res.error ?? "watch creation failed",
          })
        }
        break
      }

      case "WATCH_CANCELED": {
        // fire and forget cancel
        //  handler was already removed in cancel()
        break
      }

      case "PUT":
      case "DELETE": {
        const raw = msg.payload as RawStreamEvent
        const event = this.decodeStreamEvent(raw)
        this.handlers.get(event.watcherId)?.(event)
        break
      }

      case "ERROR": {
        const errPayload = msg.payload as serverErrorPayload
        const errMsg = errPayload?.message ?? errPayload?.cause ?? "unknown server error"

        // If there's an inflight WATCH_CREATE, this error belongs to it.
        const pending = this.pendingCreates.shift()
        if (pending) {
          pending.reject(new Error(`WatchClient: server error: ${errMsg}`))
        } else {
          this.onErrorCallback?.(errMsg)
        }
        break
      }

      case "SESSION_CLOSE": {
        // Server initiated close
        this.ws.close()
        break
      }
    }
  }

  private encodeCreateRequest(opts: WatchCreateOptions): WatchCreateRequest {
    return {
      // id: 0 / omitted for simplicities sake; server generates the watchId
      key:            strToB64(opts.key),
      end:            opts.end ? strToB64(opts.end) : undefined,
      prefix:         opts.prefix ?? false,
      start_revision: opts.startRevision ?? 0,
      prev_entry:     opts.prevEntry ?? false,
      filter:         opts.filter,
    }
  }


  private decodeStreamEvent(raw: RawStreamEvent): WatchEvent {
    return {
      watcherId: raw.watcher_id,
      kind:      raw.event.kind,
      entry:     decodeEntry(raw.event.entry),
      prevEntry: raw.event.prev_entry
        ? decodeEntry(raw.event.prev_entry)
        : undefined,
    }
  }

  private send(kind: ClientMessageKind, payload: unknown): void {
    if (this.ws.readyState !== WebSocket.OPEN) return
    const msg: ClientMessage = { kind, payload }
    this.ws.send(JSON.stringify(msg))
  }
}