import { useEffect, useRef, useState } from "react"
import {
  ActivityIcon,
  GiftIcon,
  LoaderCircleIcon,
  LogOutIcon,
  MinusIcon,
  Trash2Icon,
  UserRoundSearchIcon,
  XIcon,
} from "lucide-react"
import appIconUrl from "@/assets/app-icon.svg"

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Skeleton } from "@/components/ui/skeleton"
import { Slider } from "@/components/ui/slider"
import { Toaster } from "@/components/ui/sonner"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  bootstrapApp,
  deleteCachedAccount,
  refreshAccount,
  shutdownRun,
  startRun,
  subscribeSnapshots,
  windowControl,
} from "@/lib/bridge"
import type {
  AccountChoice,
  AppSnapshot,
  CachedAccount,
  Checkpoint,
  DropProgress,
  RunConfiguration,
} from "@/lib/contracts"
import { cn } from "@/lib/utils"
import { toast } from "sonner"

const MAX_SESSIONS = 1000
const SESSION_PRESETS = [10, 50, 100, 200, 500, 1000]

function sessionScale(value: number) {
  return Math.log(Math.max(1, Math.min(MAX_SESSIONS, value))) / Math.log(MAX_SESSIONS)
}

function sessionsFromScale(value: number) {
  return Math.max(1, Math.min(MAX_SESSIONS, Math.round(Math.exp(value * Math.log(MAX_SESSIONS)))))
}

function App() {
  const [snapshot, setSnapshot] = useState<AppSnapshot | null>(null)
  const [configuration, setConfiguration] = useState<RunConfiguration | null>(null)
  const [accountChoice, setAccountChoice] = useState<AccountChoice | null>(null)
  const [busy, setBusy] = useState(false)
  const [localError, setLocalError] = useState<string | null>(null)

  useEffect(() => {
    let active = true
    let unlisten: (() => void) | undefined
    const acceptSnapshot = (next: AppSnapshot) => {
      if (!active) return
      setSnapshot((current) => current && current.revision > next.revision ? current : next)
    }
    void (async () => {
      try {
        unlisten = await subscribeSnapshots(acceptSnapshot)
        const initial = await bootstrapApp()
        if (!active) return
        acceptSnapshot(initial.snapshot)
        setConfiguration(initial.configuration)
        setAccountChoice(initial.selectedAccount ?? null)
      } catch (error) {
        if (active) toast.error("无法启动", { description: errorMessage(error, "应用初始化失败。") })
      }
    })()
    return () => {
      active = false
      unlisten?.()
    }
  }, [])

  async function beginRun() {
    if (!configuration || !accountChoice || accountChoice.kind !== "cached") return
    setLocalError(null)
    setBusy(true)
    try {
      await startRun(configuration, accountChoice)
    } catch (error) {
      setLocalError(errorMessage(error, "无法开始运行。"))
    } finally {
      setBusy(false)
    }
  }

  async function refreshCachedAccount(replaceUid?: string) {
    if (!configuration) return
    setBusy(true)
    try {
      const uid = await refreshAccount(configuration, replaceUid)
      if (uid) setAccountChoice({ kind: "cached", uid })
    } catch (error) {
      setLocalError(errorMessage(error, "无法获取账号，请在应用内完成 B 站登录。"))
    } finally {
      setBusy(false)
    }
  }

  async function stopRun() {
    setBusy(true)
    try {
      await shutdownRun()
    } catch (error) {
      toast.error("停止失败", { description: errorMessage(error, "无法停止当前运行。") })
    } finally {
      setBusy(false)
    }
  }

  async function acknowledgeRunError() {
    setBusy(true)
    try {
      await shutdownRun()
    } catch (error) {
      setLocalError(errorMessage(error, "无法返回配置页。"))
    } finally {
      setBusy(false)
    }
  }

  async function removeAccount(uid: string) {
    try {
      await deleteCachedAccount(uid)
      if (accountChoice?.kind === "cached" && accountChoice.uid === uid) {
        setAccountChoice(null)
      }
    } catch (error) {
      toast.error("删除失败", { description: errorMessage(error, "无法删除账号。") })
    }
  }

  return (
    <TooltipProvider delayDuration={300}>
      <main className="application">
        <TitleBar />
        {!snapshot || !configuration ? (
          <AppSkeleton />
        ) : snapshot.view === "run" ? (
          <RunView
            snapshot={snapshot}
            onStop={stopRun}
            busy={busy}
          />
        ) : (
          <SetupView
            snapshot={snapshot}
            configuration={configuration}
            accountChoice={accountChoice}
            onConfigurationChange={setConfiguration}
            onAccountChoiceChange={(value) => setAccountChoice(value)}
            onRefreshAccount={refreshCachedAccount}
            onDeleteAccount={removeAccount}
            onStart={beginRun}
            busy={busy}
          />
        )}
      </main>

      {localError && <CheckErrorDialog message={localError} onConfirm={() => setLocalError(null)} />}
      {snapshot?.phase === "failed" && snapshot.diagnostics.errors[0] && (
        <CheckErrorDialog message={snapshot.diagnostics.errors[0]} onConfirm={acknowledgeRunError} />
      )}
      <Toaster position="bottom-right" />
    </TooltipProvider>
  )
}

function TitleBar() {
  return (
    <header
      className="titlebar"
      data-tauri-drag-region="deep"
    >
      <div className="titlebar-brand" data-tauri-drag-region="deep">
        <span className="brand-icon" aria-hidden="true">
          <img src={appIconUrl} alt="" draggable={false} />
        </span>
        <span>BiliOwDrops</span>
      </div>
      <div className="window-actions">
        <Tooltip>
          <TooltipTrigger asChild>
            <button aria-label="最小化" onClick={() => void windowControl("minimize")}><MinusIcon /></button>
          </TooltipTrigger>
          <TooltipContent>最小化</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <button className="window-close" aria-label="关闭" onClick={() => void windowControl("close")}><XIcon /></button>
          </TooltipTrigger>
          <TooltipContent>关闭</TooltipContent>
        </Tooltip>
      </div>
    </header>
  )
}

type SetupViewProps = {
  snapshot: AppSnapshot
  configuration: RunConfiguration
  accountChoice: AccountChoice | null
  onConfigurationChange: (value: RunConfiguration) => void
  onAccountChoiceChange: (value: AccountChoice) => void
  onRefreshAccount: (replaceUid?: string) => void
  onDeleteAccount: (uid: string) => void
  onStart: () => void
  busy: boolean
}

function SetupView({
  snapshot,
  configuration,
  accountChoice,
  onConfigurationChange,
  onAccountChoiceChange,
  onRefreshAccount,
  onDeleteAccount,
  onStart,
  busy,
}: SetupViewProps) {
  const canStart = accountChoice?.kind === "cached" && ["idle", "failed"].includes(snapshot.phase)
  const preparing = !["idle", "failed"].includes(snapshot.phase)
  const setupRef = useRef<HTMLDivElement>(null)
  const draggingDivider = useRef(false)
  const [accountPaneWidth, setAccountPaneWidth] = useState(323)
  const [dividerDragging, setDividerDragging] = useState(false)

  useEffect(() => {
    const onPointerMove = (event: PointerEvent) => {
      if (!draggingDivider.current || !setupRef.current) return
      const bounds = setupRef.current.getBoundingClientRect()
      const nextWidth = Math.round(event.clientX - bounds.left)
      const maxWidth = Math.max(250, bounds.width - 480)
      setAccountPaneWidth(Math.max(250, Math.min(maxWidth, nextWidth)))
    }
    const onPointerUp = () => {
      if (!draggingDivider.current) return
      draggingDivider.current = false
      setDividerDragging(false)
    }
    window.addEventListener("pointermove", onPointerMove)
    window.addEventListener("pointerup", onPointerUp)
    return () => {
      window.removeEventListener("pointermove", onPointerMove)
      window.removeEventListener("pointerup", onPointerUp)
    }
  }, [])

  useEffect(() => {
    const element = setupRef.current
    if (!element) return
    const observer = new ResizeObserver(([entry]) => {
      const maxWidth = Math.max(250, entry.contentRect.width - 480)
      setAccountPaneWidth((current) => Math.min(current, maxWidth))
    })
    observer.observe(element)
    return () => observer.disconnect()
  }, [])

  function adjustDivider(delta: number) {
    const bounds = setupRef.current?.getBoundingClientRect()
    const maxWidth = bounds ? Math.max(250, bounds.width - 480) : 323
    setAccountPaneWidth((current) => Math.max(250, Math.min(maxWidth, current + delta)))
  }

  return (
    <div
      ref={setupRef}
      className="setup-screen"
      style={{ gridTemplateColumns: `${accountPaneWidth}px 10px minmax(470px, 1fr)` }}
    >
      <section className="account-pane">
        <PaneHeading label="账号" />
        <AccountSelector
          accounts={snapshot.accounts}
          value={accountChoice}
          onChange={onAccountChoiceChange}
          onAddAccount={() => onRefreshAccount()}
          onDelete={onDeleteAccount}
          busy={busy}
        />
      </section>

      <div
        className={cn("setup-divider", dividerDragging && "is-dragging")}
        role="separator"
        aria-orientation="vertical"
        aria-label="调整账号和设置宽度"
        aria-valuemin={250}
        aria-valuemax={Math.max(250, accountPaneWidth)}
        aria-valuenow={accountPaneWidth}
        tabIndex={0}
        onPointerDown={(event) => {
          event.preventDefault()
          draggingDivider.current = true
          setDividerDragging(true)
        }}
        onKeyDown={(event) => {
          if (event.key === "ArrowLeft") {
            event.preventDefault()
            adjustDivider(-16)
          } else if (event.key === "ArrowRight") {
            event.preventDefault()
            adjustDivider(16)
          }
        }}
      />

      <section className="settings-pane">
        <PaneHeading label="运行设置" />
        <div className="settings-form">
          <label className="control-field" htmlFor="room-id">
            <span>直播间号</span>
            <Input
              id="room-id"
              value={configuration.roomId}
              inputMode="numeric"
              onChange={(event) => onConfigurationChange({ ...configuration, roomId: event.target.value })}
              spellCheck={false}
            />
          </label>

          <div className="concurrency-control">
            <div className="concurrency-header">
              <label htmlFor="session-count">并发</label>
              <div className="concurrency-value">
                <Input
                  id="session-count"
                  type="number"
                  min={1}
                  max={MAX_SESSIONS}
                  value={configuration.sessions}
                  onChange={(event) => {
                    const value = Math.max(1, Math.min(MAX_SESSIONS, Number(event.target.value) || 1))
                    onConfigurationChange({ ...configuration, sessions: value })
                  }}
                  aria-label="并发数"
                />
                <span>/ {MAX_SESSIONS}</span>
              </div>
            </div>
            <Slider
              value={[sessionScale(configuration.sessions)]}
              min={0}
              max={1}
              step={0.001}
              onValueChange={([value]) => onConfigurationChange({ ...configuration, sessions: sessionsFromScale(value) })}
              aria-label="并发数"
            />
            <div className="preset-row" aria-label="常用并发数">
              {SESSION_PRESETS.map((preset) => (
                <Button
                  key={preset}
                  variant={configuration.sessions === preset ? "secondary" : "ghost"}
                  size="sm"
                  style={{ left: `${sessionScale(preset) * 100}%` }}
                  onClick={() => onConfigurationChange({ ...configuration, sessions: preset })}
                >
                  {preset}
                </Button>
              ))}
            </div>
          </div>
        </div>
        <div className="settings-actions">
          <Button className="launch-button" size="lg" onClick={onStart} disabled={busy || !canStart}>
            {busy || preparing ? <LoaderCircleIcon className="animate-spin" data-icon="inline-start" /> : <ActivityIcon data-icon="inline-start" />}
            {preparing ? "准备中" : "开始"}
          </Button>
        </div>
      </section>
    </div>
  )
}

function PaneHeading({ label }: { label: string }) {
  return (
    <div className="pane-heading">
      <h1>{label}</h1>
    </div>
  )
}

function AccountSelector({
  accounts,
  value,
  onChange,
  onAddAccount,
  onDelete,
  busy,
}: {
  accounts: CachedAccount[]
  value: AccountChoice | null
  onChange: (value: AccountChoice) => void
  onAddAccount: () => void
  onDelete: (uid: string) => void
  busy: boolean
}) {
  return (
    <div className="account-selector" role="radiogroup" aria-label="运行账号">
      {accounts.length === 0 ? (
        <div className="account-empty" role="status">
          <strong>暂无账号</strong>
        </div>
      ) : (
        <ScrollArea className="account-scroll">
          <div className="account-list">
            {accounts.map((account) => {
              const selected = value?.kind === "cached" && value.uid === account.uid
              const displayName = account.displayName?.trim() || account.uid
              const hasDisplayName = displayName !== account.uid
              return (
                <div className="account-entry" key={account.uid}>
                  <button
                    type="button"
                    className={cn("account-row", selected && "is-selected")}
                    role="radio"
                    aria-checked={selected}
                    onClick={() => onChange({ kind: "cached", uid: account.uid })}
                    aria-label={`${displayName}，UID ${account.uid}`}
                  >
                    <span className="account-copy">
                      <strong>{displayName}</strong>
                      {hasDisplayName && <small>{account.uid}</small>}
                    </span>
                  </button>
                  <span className="account-actions">
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button
                          variant="ghost"
                          size="icon-sm"
                          aria-label={`删除账号 ${account.uid}`}
                          onClick={() => onDelete(account.uid)}
                        >
                          <Trash2Icon />
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>删除</TooltipContent>
                    </Tooltip>
                  </span>
                </div>
              )
            })}
          </div>
        </ScrollArea>
      )}

      <button
        type="button"
        className="account-add"
        disabled={busy}
        onClick={onAddAccount}
      >
        <UserRoundSearchIcon aria-hidden="true" />
        <span>登录并获取 B 站账号</span>
      </button>
    </div>
  )
}

function CheckErrorDialog({
  message,
  onConfirm,
}: {
  message: string
  onConfirm: () => void | Promise<void>
}) {
  const [submitting, setSubmitting] = useState(false)

  async function confirm() {
    if (submitting) return
    setSubmitting(true)
    try {
      await onConfirm()
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <AlertDialog open>
      <AlertDialogContent className="app-dialog check-error-dialog">
        <AlertDialogHeader>
          <AlertDialogTitle>检查失败</AlertDialogTitle>
          <AlertDialogDescription>{message}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter className="app-dialog-actions check-error-actions">
          <AlertDialogAction disabled={submitting} onClick={() => void confirm()}>确定</AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}

function RunView({
  snapshot,
  onStop,
  busy,
}: {
  snapshot: AppSnapshot
  onStop: () => void
  busy: boolean
}) {
  const progresses = snapshot.progress
  const [activeTaskKey, setActiveTaskKey] = useState<string | null>(null)
  useEffect(() => {
    setActiveTaskKey((current) => {
      if (current && progresses.some((progress) => progress.taskKey === current)) return current
      return progresses[0]?.taskKey ?? null
    })
  }, [progresses])

  const activeProgress = progresses.find((progress) => progress.taskKey === activeTaskKey) ?? progresses[0]
  const showTabs = progresses.length > 0
  const hasMultiple = progresses.length > 1
  const dropsPending = !["idle_drops", "completed", "failed"].includes(snapshot.phase)
  const account = snapshot.identity && snapshot.accounts.find((item) => item.uid === snapshot.identity?.uid)
  const displayName = account?.displayName?.trim() || snapshot.identity?.uid || "读取中"
  return (
    <div className="run-screen">
      <header className="run-summary">
        <div className="identity">
          <div className="identity-block">
            <span className="identity-label">用户名</span>
            <strong>{displayName}</strong>
          </div>
          <div className="identity-block">
            <span className="identity-label">UID</span>
            <strong>{snapshot.identity?.uid ?? "读取中"}</strong>
          </div>
          <div className="identity-block">
            <span className="identity-label">房间号</span>
            <strong>{snapshot.identity?.roomId ?? "读取中"}</strong>
          </div>
        </div>
        <div className={cn("run-state", `is-${snapshot.phase}`)}><i />{phaseLabel(snapshot.phase)}</div>
      </header>

      <section className={cn("mission-progress", showTabs && "has-tabs", hasMultiple && "has-multiple")}>
        {progresses.length === 0 ? (
          <div className="progress-stage">
            <div className={cn("progress-empty", snapshot.phase === "idle_drops" ? "is-idle" : "is-loading")}>
              {dropsPending ? (
                <div className="loading-state" aria-live="polite">
                  <span className="loading-spinner" aria-hidden="true" />
                  <span className="loading-label">读取中</span>
                </div>
              ) : snapshot.phase === "idle_drops" ? (
                <div className="idle-state" aria-live="polite">
                  <div className="idle-copy">
                    <GiftIcon className="idle-icon" aria-hidden="true" />
                    <strong>暂无掉宝</strong>
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        ) : (
          <div className={cn("progress-stage", showTabs && "has-tabs")}>
            {showTabs && (
              <div className="drop-tabs" role="tablist" aria-label="今天的掉宝">
                {progresses.map((progress) => {
                  const selected = progress.taskKey === activeProgress?.taskKey
                  return (
                    <button
                      key={progress.taskKey}
                      type="button"
                      className={cn("drop-tab", selected && "is-active")}
                      role="tab"
                      aria-selected={selected}
                      tabIndex={selected ? 0 : -1}
                      onClick={() => setActiveTaskKey(progress.taskKey)}
                    >
                      <span>{progress.name}</span>
                    </button>
                  )
                })}
              </div>
            )}
            {activeProgress && <DropProgressRow progress={activeProgress} showName={false} />}
          </div>
        )}
      </section>

      <section className="session-line">
        <div><span>会话</span><strong>{snapshot.sessions.established}<small>/ {snapshot.sessions.target}</small></strong></div>
        <div><span>健康</span><strong>{snapshot.sessions.healthy}</strong></div>
        <div><span>心跳</span><strong>{snapshot.sessions.heartbeats}</strong></div>
        <div><span>速度</span><strong>{snapshot.diagnostics.rate == null ? "--" : snapshot.diagnostics.rate.toFixed(1)}{snapshot.diagnostics.rate != null && <small> / 分钟</small>}</strong></div>
        <div><span>重连</span><strong>{snapshot.sessions.reconnects}</strong></div>
      </section>

      <footer className="run-actionbar">
        <Button className="launch-button return-button" size="lg" onClick={onStop} disabled={busy}>
          {busy ? <LoaderCircleIcon className="animate-spin" data-icon="inline-start" /> : <LogOutIcon data-icon="inline-start" />}
          返回
        </Button>
      </footer>
    </div>
  )
}

function DropProgressRow({ progress, showName }: { progress: DropProgress; showName: boolean }) {
  const percent = progress.limit > 0 ? Math.min(100, Math.max(0, (progress.current / progress.limit) * 100)) : 0
  return (
    <div className="drop-progress-row">
      <div className="progress-copy">
        {showName && <span className="drop-name">{progress.name}</span>}
        <strong>
          <span className="progress-current">{formatNumber(progress.current)}</span>
          <small>/ {formatNumber(progress.limit)} 分钟</small>
        </strong>
        <em>{`${percent.toFixed(1)}%`}</em>
      </div>
      <div className="progress-rail" aria-label={`${progress.name} ${percent.toFixed(1)}%`}>
        <div className="progress-track">
          <div className="progress-fill" style={{ width: `${percent}%` }} />
          {progress.checkpoints.map((checkpoint) => (
            <CheckpointMarker key={checkpoint.key} checkpoint={checkpoint} limit={progress.limit} />
          ))}
        </div>
      </div>
    </div>
  )
}

function CheckpointMarker({ checkpoint, limit }: { checkpoint: Checkpoint; limit: number }) {
  const left = limit > 0 ? Math.min(100, Math.max(0, (checkpoint.limit / limit) * 100)) : 0
  return (
    <span
      className={cn(
        "checkpoint-marker",
        `is-${checkpoint.state}`,
      )}
      style={{ left: `${left}%` }}
      aria-label={`${formatNumber(checkpoint.limit)} 分钟 ${checkpointStateLabel(checkpoint.state)}`}
    >
      <span className="checkpoint-dot" />
      <span className="checkpoint-label">
        <strong>{formatNumber(checkpoint.limit)}</strong>
        <small>{checkpointStateLabel(checkpoint.state)}</small>
      </span>
    </span>
  )
}

function AppSkeleton() {
  return (
    <div className="app-skeleton">
      <Skeleton className="h-full w-[38%]" />
      <Skeleton className="h-full flex-1" />
    </div>
  )
}

function checkpointStateLabel(state: Checkpoint["state"]) {
  return state === "claimed" ? "已领取" : state === "claimable" ? "可领取" : "未完成"
}

function phaseLabel(phase: AppSnapshot["phase"]) {
  const labels: Record<AppSnapshot["phase"], string> = {
    idle: "等待开始",
    validating_account: "读取中",
    reading_identity: "读取账号",
    resolving_room: "读取中",
    reading_drops: "读取中",
    idle_drops: "空闲中",
    connecting_sessions: "运行中",
    running: "运行中",
    stopping: "正在停止",
    completed: "已完成",
    failed: "运行异常",
  }
  return labels[phase]
}

function formatNumber(value: number) {
  return Number.isInteger(value) ? String(value) : value.toFixed(1)
}

function errorMessage(error: unknown, fallback: string) {
  if (typeof error === "string" && error.trim()) return error
  if (error instanceof Error && error.message.trim()) return error.message
  return fallback
}

export default App
