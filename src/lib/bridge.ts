import { invoke } from "@tauri-apps/api/core"
import { listen } from "@tauri-apps/api/event"
import { getCurrentWindow } from "@tauri-apps/api/window"

import type { AccountChoice, AppSnapshot, BootstrapPayload, RunConfiguration } from "@/lib/contracts"

export async function bootstrapApp(): Promise<BootstrapPayload> {
  return invoke<BootstrapPayload>("bootstrap_app")
}

export async function subscribeSnapshots(handler: (snapshot: AppSnapshot) => void) {
  const unlisten = await listen<AppSnapshot>("app-snapshot", (event) => handler(event.payload))
  return unlisten
}

export async function startRun(configuration: RunConfiguration, account: AccountChoice) {
  await invoke("start_run", { configuration, account })
}

export async function refreshAccount(configuration: RunConfiguration, replaceUid?: string) {
  return invoke<string | null>("refresh_account", { configuration, replaceUid })
}

export async function shutdownRun() {
  await invoke("shutdown_run")
}

export async function deleteCachedAccount(uid: string) {
  await invoke("delete_cached_account", { uid })
}

export async function windowControl(action: "minimize" | "toggleMaximize" | "close") {
  const window = getCurrentWindow()
  if (action === "minimize") return window.minimize()
  if (action === "toggleMaximize") return window.toggleMaximize()
  return window.close()
}
