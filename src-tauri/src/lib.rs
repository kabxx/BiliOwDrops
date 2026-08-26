mod app;
mod bilibili;
mod cache;
mod domain;
mod login;
#[cfg(windows)]
mod platform;
mod rewards;
mod tasks;
mod watch_manager;
mod watch_protocol;

use std::sync::Arc;

use app::AppController;
use domain::{AccountChoice, AppSnapshot, RunConfiguration};
use serde::Serialize;
use tauri::{Manager, State, WindowEvent};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapPayload {
    snapshot: AppSnapshot,
    configuration: RunConfiguration,
    selected_account: Option<AccountChoice>,
}

#[tauri::command]
fn bootstrap_app(controller: State<'_, Arc<AppController>>) -> BootstrapPayload {
    controller.bootstrap()
}

#[tauri::command]
async fn start_run(
    controller: State<'_, Arc<AppController>>,
    configuration: RunConfiguration,
    account: AccountChoice,
) -> Result<(), String> {
    controller
        .inner()
        .start(configuration, account)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn refresh_account(
    controller: State<'_, Arc<AppController>>,
    configuration: RunConfiguration,
    replace_uid: Option<String>,
) -> Result<Option<String>, String> {
    controller
        .inner()
        .refresh_account(configuration, replace_uid)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn shutdown_run(controller: State<'_, Arc<AppController>>) -> Result<(), String> {
    controller.stop().await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn delete_cached_account(
    controller: State<'_, Arc<AppController>>,
    uid: String,
) -> Result<(), String> {
    controller
        .delete_account(&uid)
        .await
        .map_err(|error| error.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.unminimize();
                let _ = window.set_focus();
            }
        }))
        .setup(|app| {
            let controller = AppController::new(app.handle().clone())?;
            app.manage(Arc::clone(&controller));
            tauri::async_runtime::spawn(controller.hydrate_account_names());

            #[cfg(windows)]
            if let Some(window) = app.get_webview_window("main") {
                let icons = platform::windows::window_icon::install(&window)?;
                app.manage(icons);

                let icon_window = window.clone();
                window.on_window_event(move |event| {
                    if matches!(event, WindowEvent::ScaleFactorChanged { .. }) {
                        let icons = icon_window
                            .state::<platform::windows::window_icon::WindowIconManager>();
                        let _ = icons.refresh(&icon_window);
                    }
                });
            }

            if let Some(window) = app.get_webview_window("main") {
                window.show()?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            bootstrap_app,
            start_run,
            refresh_account,
            shutdown_run,
            delete_cached_account,
        ])
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                if window.label() != "main" {
                    return;
                }
                let app = window.app_handle().clone();
                let label = window.label().to_string();
                api.prevent_close();
                let Some(controller) = app.try_state::<Arc<AppController>>() else {
                    return;
                };
                if !controller.begin_window_close() {
                    return;
                }
                let controller = Arc::clone(controller.inner());
                tauri::async_runtime::spawn(async move {
                    let _ =
                        tokio::time::timeout(std::time::Duration::from_secs(12), controller.stop())
                            .await;
                    if let Some(window) = app.get_webview_window(&label) {
                        let _ = window.destroy();
                    }
                    app.exit(0);
                });
            }
        })
        .run(tauri::generate_context!())
        .expect("无法启动 BiliOwDrops");
}
