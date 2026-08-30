"""Cross-platform native directory picker."""

import os
import platform
import subprocess

_WINDOWS_SCRIPT = """
Add-Type -AssemblyName System.Windows.Forms
$f = New-Object System.Windows.Forms.FolderBrowserDialog
$f.Description = $env:PYSUS_DIALOG_TITLE
$f.SelectedPath = $env:PYSUS_DIALOG_INITIALDIR
$f.ShowDialog() | Out-Null
$f.SelectedPath
"""

_MACOS_SCRIPT = """
set dialogTitle to system attribute "PYSUS_DIALOG_TITLE"
set initialDirectory to system attribute "PYSUS_DIALOG_INITIALDIR"
tell application "System Events"
    activate
    set f to choose folder with prompt dialogTitle ¬
        default location POSIX file initialDirectory
    POSIX path of f
end tell
"""


def _dialog_environment(title: str, initialdir: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PYSUS_DIALOG_TITLE"] = title
    env["PYSUS_DIALOG_INITIALDIR"] = initialdir
    return env


def native_dir_picker(title: str, initialdir: str) -> str:
    """Open a native directory picker and return the selected path.

    Values are passed as command arguments or environment variables instead
    of being interpolated into scripts executed by platform interpreters.
    """
    system = platform.system()

    if system == "Linux":
        for cmd in (
            [
                "zenity",
                "--file-selection",
                "--directory",
                f"--filename={initialdir}/",
                f"--title={title}",
            ],
            ["kdialog", "--getexistingdirectory", initialdir, "--title", title],
        ):
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30
                )
                return result.stdout.strip()
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue

    elif system == "Windows":
        result = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                _WINDOWS_SCRIPT,
            ],
            capture_output=True,
            text=True,
            env=_dialog_environment(title, initialdir),
        )
        return result.stdout.strip()

    elif system == "Darwin":
        result = subprocess.run(
            ["osascript", "-e", _MACOS_SCRIPT],
            capture_output=True,
            text=True,
            env=_dialog_environment(title, initialdir),
        )
        return result.stdout.strip()

    return ""
