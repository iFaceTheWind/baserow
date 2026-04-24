import { vi, describe, beforeEach, test, expect } from 'vitest'

import { RealTimeHandler } from '@baserow/modules/core/plugins/realTimeHandler'

vi.mock('#imports', () => ({
  useRuntimeConfig: () => ({
    public: { publicBackendUrl: 'http://localhost' },
  }),
}))

// The handler reads WebSocket.OPEN / CONNECTING constants; the test
// environment may not provide them on the class object, in which case the
// readiness gate would short-circuit and silently swallow sends.
if (typeof globalThis.WebSocket === 'undefined') {
  globalThis.WebSocket = {}
}
if (typeof globalThis.WebSocket.OPEN !== 'number') {
  globalThis.WebSocket.OPEN = 1
  globalThis.WebSocket.CONNECTING = 0
  globalThis.WebSocket.CLOSING = 2
  globalThis.WebSocket.CLOSED = 3
}

function makeStore(initialWorkspaceId = null) {
  let selected = initialWorkspaceId
    ? { id: initialWorkspaceId, _: { type: 'workspace' } }
    : {}
  const dispatched = []
  const subscribers = []
  const store = {
    getters: {
      'auth/token': 'token',
      'workspace/getSelected': () => selected,
    },
    dispatch(name, value) {
      dispatched.push([name, value])
      return Promise.resolve()
    },
    subscribe(fn) {
      subscribers.push(fn)
    },
    _setSelected(workspace) {
      selected = workspace || {}
      for (const sub of subscribers) {
        sub({ type: 'workspace/SET_SELECTED' })
      }
    },
    _dispatched: dispatched,
  }
  // Vuex-style getter that's read like a property, not invoked. The handler
  // does `store.getters['workspace/getSelected']` and uses its `id` directly.
  Object.defineProperty(store.getters, 'workspace/getSelected', {
    get() {
      return selected
    },
  })
  return store
}

function makeHandler({ workspaceId = null } = {}) {
  const store = makeStore(workspaceId)
  const context = { store, app: { router: {} } }
  const handler = new RealTimeHandler(context)
  // Stand-in for an open websocket so _sendWorkspaceRealtimeSubscribe goes
  // through.
  const sentMessages = []
  handler.socket = {
    readyState: 1, // WebSocket.OPEN
    send(payload) {
      sentMessages.push(JSON.parse(payload))
    },
  }
  return { handler, store, context, sentMessages }
}

function fire(handler, type, data) {
  for (const cb of handler.events[type] || []) {
    cb(handler.context, data)
  }
}

describe('RealTimeHandler workspace_realtime_subscribe flow', () => {
  let env
  beforeEach(() => {
    env = makeHandler({ workspaceId: 7 })
  })

  test('authentication sends a baseline subscribe on initial connect', () => {
    fire(env.handler, 'authentication', {
      web_socket_id: 'ws-1',
      success: true,
    })
    expect(env.handler.currentWebSocketId).toBe('ws-1')
    const subscribe = env.sentMessages.find(
      (m) => m.type === 'workspace_realtime_subscribe'
    )
    expect(subscribe).toEqual({
      type: 'workspace_realtime_subscribe',
      workspace_id: 7,
      last_seen_id: null,
      previous_web_socket_id: null,
    })
  })

  test('authentication does not subscribe when no workspace is active', () => {
    const local = makeHandler({ workspaceId: null })
    fire(local.handler, 'authentication', {
      web_socket_id: 'ws-x',
      success: true,
    })
    expect(local.sentMessages).toEqual([])
  })

  test('subscribe_result with has_updates and newer id fires the toast', () => {
    env.handler.lastSeenRealtimeUpdateId = 10
    env.handler.lastSeenWorkspaceId = 7
    fire(env.handler, 'workspace_realtime_subscribe_result', {
      workspace_id: 7,
      has_updates: true,
      current_latest_id: 42,
    })
    expect(
      env.store._dispatched.some(
        ([n, v]) => n === 'toast/setWorkspaceStale' && v === true
      )
    ).toBe(true)
    expect(env.handler.lastSeenRealtimeUpdateId).toBe(42)
    expect(env.handler.previousWebSocketId).toBeNull()
  })

  test('subscribe_result with has_updates but stale current_latest_id does not toast', () => {
    // Mimics a race where a fresh broadcast advanced lastSeen above
    // current_latest_id before the response arrived.
    env.handler.lastSeenRealtimeUpdateId = 50
    env.handler.lastSeenWorkspaceId = 7
    fire(env.handler, 'workspace_realtime_subscribe_result', {
      workspace_id: 7,
      has_updates: true,
      current_latest_id: 42,
    })
    expect(
      env.store._dispatched.some(
        ([n, v]) => n === 'toast/setWorkspaceStale' && v === true
      )
    ).toBe(false)
    // The high-water mark must not regress below the live-message value.
    expect(env.handler.lastSeenRealtimeUpdateId).toBe(50)
  })

  test('subscribe_result with has_updates=false advances baseline without toast', () => {
    env.handler.lastSeenWorkspaceId = 7
    fire(env.handler, 'workspace_realtime_subscribe_result', {
      workspace_id: 7,
      has_updates: false,
      current_latest_id: 99,
    })
    expect(
      env.store._dispatched.some(
        ([n, v]) => n === 'toast/setWorkspaceStale' && v === true
      )
    ).toBe(false)
    expect(env.handler.lastSeenRealtimeUpdateId).toBe(99)
  })

  test('subscribe_result for an inactive workspace is ignored', () => {
    fire(env.handler, 'workspace_realtime_subscribe_result', {
      workspace_id: 999, // not the active workspace (7)
      has_updates: true,
      current_latest_id: 42,
    })
    expect(env.handler.lastSeenRealtimeUpdateId).toBeNull()
    expect(
      env.store._dispatched.some(([n]) => n === 'toast/setWorkspaceStale')
    ).toBe(false)
  })
})

describe('RealTimeHandler high-water mark', () => {
  test('_advanceLastSeenRealtimeUpdateId takes the max of incoming ids', () => {
    const { handler } = makeHandler({ workspaceId: 1 })
    handler._advanceLastSeenRealtimeUpdateId({ realtime_update_id: 5 })
    expect(handler.lastSeenRealtimeUpdateId).toBe(5)
    handler._advanceLastSeenRealtimeUpdateId({ realtime_update_id: 3 })
    expect(handler.lastSeenRealtimeUpdateId).toBe(5)
    handler._advanceLastSeenRealtimeUpdateId({ realtime_update_id: 7 })
    expect(handler.lastSeenRealtimeUpdateId).toBe(7)
    handler._advanceLastSeenRealtimeUpdateId({ type: 'no_id' })
    expect(handler.lastSeenRealtimeUpdateId).toBe(7)
  })
})

describe('RealTimeHandler workspace switch', () => {
  test('switching to a new workspace resets baseline and re-subscribes', () => {
    const { handler, store, sentMessages } = makeHandler({ workspaceId: 7 })
    // Connect+authenticate first to capture web_socket_id.
    fire(handler, 'authentication', { web_socket_id: 'ws-1', success: true })
    handler.lastSeenRealtimeUpdateId = 33
    sentMessages.length = 0

    store._setSelected({ id: 12, _: { type: 'workspace' } })

    expect(handler.lastSeenRealtimeUpdateId).toBeNull()
    expect(handler.previousWebSocketId).toBeNull()
    const subscribe = sentMessages.find(
      (m) => m.type === 'workspace_realtime_subscribe'
    )
    expect(subscribe).toEqual({
      type: 'workspace_realtime_subscribe',
      workspace_id: 12,
      last_seen_id: null,
      previous_web_socket_id: null,
    })
  })
})
