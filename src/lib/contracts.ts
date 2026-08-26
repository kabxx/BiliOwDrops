export type AppPhase =
  | "idle"
  | "validating_account"
  | "reading_identity"
  | "resolving_room"
  | "reading_drops"
  | "idle_drops"
  | "connecting_sessions"
  | "running"
  | "stopping"
  | "completed"
  | "failed"

export type CheckpointState = "pending" | "claimable" | "claimed"

export type Checkpoint = {
  key: string
  current: number
  limit: number
  state: CheckpointState
}

export type DropProgress = {
  taskKey: string
  name: string
  current: number
  limit: number
  sampledAt: string
  checkpoints: Checkpoint[]
}

export type CachedAccount = {
  uid: string
  displayName?: string
  roomId: number
  lastUsedAt: string
}

export type AccountChoice =
  | { kind: "cached"; uid: string }
  | { kind: "refresh"; replaceUid?: string }

export type RunConfiguration = {
  roomId: string
  sessions: number
}

export type AppSnapshot = {
  revision: number
  view: "setup" | "run"
  phase: AppPhase
  phaseMessage: string
  startedAt?: string
  accounts: CachedAccount[]
  identity?: {
    uid: string
    roomId: number
  }
  progress: DropProgress[]
  sessions: {
    target: number
    registered: number
    established: number
    healthy: number
    heartbeats: number
    reconnects: number
    rateLimits: number
  }
  diagnostics: {
    rate?: number
    updatedAt?: string
    errors: string[]
  }
}

export type BootstrapPayload = {
  snapshot: AppSnapshot
  configuration: RunConfiguration
  selectedAccount?: AccountChoice
}
