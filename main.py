import threading
import time
import sys
import os

# ── path setup ───────────────────────────────────────────────────────────────
APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, APP_DIR)

# On Android, store data files in the app's writable data directory
try:
    from android.storage import app_storage_path  # type: ignore
    DATA_DIR = app_storage_path()
except ImportError:
    DATA_DIR = APP_DIR  # desktop fallback

os.environ["TG_DATA_DIR"] = DATA_DIR

# ── Kivy config (must be before kivy imports) ────────────────────────────────
os.environ.setdefault("KIVY_NO_ENV_CONFIG", "1")

from kivy.app import App                          # noqa: E402
from kivy.uix.boxlayout import BoxLayout          # noqa: E402
from kivy.uix.label import Label                  # noqa: E402
from kivy.clock import Clock, mainthread          # noqa: E402
from kivy.logger import Logger                    # noqa: E402
from kivy.core.window import Window               # noqa: E402

SERVER_URL = "http://127.0.0.1:5050"
_server_started = threading.Event()
_wake_lock = None


def _is_android():
    import platform
    return platform.system() == "Linux" and "ANDROID_ROOT" in os.environ


# ── Keep the app (and the backup run inside it) alive ────────────────────────
# Android can suspend the CPU or kill the process once the screen locks /
# the app goes to the background, which would silently abort a running job.
# A partial wake lock keeps the CPU awake; keep_screen_on keeps the screen
# from locking while the app is in the foreground.

def _acquire_wakelock():
    global _wake_lock
    if not _is_android() or _wake_lock is not None:
        return
    try:
        from jnius import autoclass  # type: ignore

        PythonActivity = autoclass("org.kivy.android.PythonActivity")
        Context = autoclass("android.content.Context")
        PowerManager = autoclass("android.os.PowerManager")

        activity = PythonActivity.mActivity
        power_service = activity.getSystemService(Context.POWER_SERVICE)
        lock = power_service.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "TGBackup:run")
        lock.setReferenceCounted(False)
        lock.acquire()
        _wake_lock = lock
        Logger.info("TGBackup: wake lock acquired, run will keep going in the background")
    except Exception as exc:
        Logger.warning(f"TGBackup: could not acquire wake lock ({exc})")


def _release_wakelock():
    global _wake_lock
    if _wake_lock is None:
        return
    try:
        _wake_lock.release()
    except Exception:
        pass
    _wake_lock = None


# ── Flask server thread ───────────────────────────────────────────────────────

def _run_flask():
    from server import create_app
    flask_app = create_app()
    _server_started.set()
    while True:
        try:
            flask_app.run(host="127.0.0.1", port=5050, debug=False, use_reloader=False)
        except OSError as exc:
            if "Address already in use" in str(exc):
                Logger.warning("TGBackup: port 5050 in use, retrying in 2s")
                time.sleep(2)
            else:
                Logger.error(f"TGBackup: Flask OSError: {exc}, restarting in 3s")
                time.sleep(3)
        except Exception as exc:
            Logger.error(f"TGBackup: Flask crashed: {exc}, restarting in 3s")
            time.sleep(3)


# ── Android WebView helper ────────────────────────────────────────────────────

def _open_native_webview():
    """Replace the Kivy surface with a full-screen Android WebView."""
    try:
        from jnius import autoclass                       # type: ignore
        from android.runnable import run_on_ui_thread    # type: ignore

        WebView         = autoclass("android.webkit.WebView")
        WebSettings     = autoclass("android.webkit.WebSettings")
        WebViewClient   = autoclass("android.webkit.WebViewClient")
        PythonActivity  = autoclass("org.kivy.android.PythonActivity")
        LayoutParams    = autoclass("android.view.ViewGroup$LayoutParams")

        @run_on_ui_thread
        def _setup():
            activity = PythonActivity.mActivity
            wv = WebView(activity)

            settings = wv.getSettings()
            settings.setJavaScriptEnabled(True)
            settings.setDomStorageEnabled(True)
            settings.setLoadWithOverviewMode(True)
            settings.setUseWideViewPort(True)
            settings.setBuiltInZoomControls(False)
            settings.setDisplayZoomControls(False)
            settings.setSupportZoom(False)

            wv.setWebViewClient(WebViewClient())
            wv.loadUrl(SERVER_URL)

            lp = LayoutParams(LayoutParams.MATCH_PARENT, LayoutParams.MATCH_PARENT)
            activity.setContentView(wv, lp)

        _setup()

    except Exception as exc:
        Logger.warning(f"TGBackup: WebView setup failed ({exc}), using browser fallback")
        _open_browser_fallback()


def _open_browser_fallback():
    """Open the system browser as a fallback (desktop or devices without jnius)."""
    import webbrowser
    webbrowser.open(SERVER_URL)


# ── Kivy App ─────────────────────────────────────────────────────────────────

class TGBackupApp(App):
    def on_start(self):
        Window.keep_screen_on = True
        _acquire_wakelock()

    def on_stop(self):
        _release_wakelock()

    def on_pause(self):
        # Keep running in the background instead of being stopped, so the
        # backup job in the Flask thread doesn't get cut off.
        return True

    def on_resume(self):
        pass

    def build(self):
        self.title = "TG Backup"

        self.root_layout = BoxLayout(orientation="vertical")
        self.status_label = Label(
            text="Starting server…",
            font_size="16sp",
            color=(0.5, 0.7, 1, 1),
        )
        self.root_layout.add_widget(self.status_label)

        # Start Flask in background thread
        threading.Thread(target=_run_flask, daemon=True).start()

        # Poll until server is ready, then switch to WebView
        Clock.schedule_interval(self._check_server, 0.5)
        return self.root_layout

    def _check_server(self, dt):
        if not _server_started.is_set():
            return  # still starting

        Clock.unschedule(self._check_server)
        self._switch_to_webview(dt)

    @mainthread
    def _switch_to_webview(self, *_):
        self.status_label.text = "Opening interface…"
        try:
            if _is_android():
                _open_native_webview()
            else:
                # Desktop: just open browser
                _open_browser_fallback()
                self.status_label.text = f"Server running at {SERVER_URL}\nOpen your browser."
        except Exception as exc:
            self.status_label.text = f"Error: {exc}"


if __name__ == "__main__":
    TGBackupApp().run()
