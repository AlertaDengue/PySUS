import subprocess
from unittest.mock import Mock, patch

from pysus.native_dir_picker import native_dir_picker

_TITLE = "Selecionar exportacao da unidade 'APS Central'"
_INITIALDIR = 'C:\\Dados APS\\Unidade "Central"'


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="Windows")
def test_windows_values_are_passed_through_environment(_, run):
    run.return_value = Mock(stdout="C:\\selected\n")

    assert native_dir_picker(_TITLE, _INITIALDIR) == "C:\\selected"

    args = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert _TITLE not in args[-1]
    assert _INITIALDIR not in args[-1]
    assert kwargs["env"]["PYSUS_DIALOG_TITLE"] == _TITLE
    assert kwargs["env"]["PYSUS_DIALOG_INITIALDIR"] == _INITIALDIR


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="Darwin")
def test_macos_values_are_passed_through_environment(_, run):
    run.return_value = Mock(stdout="/tmp/selected\n")

    assert native_dir_picker(_TITLE, _INITIALDIR) == "/tmp/selected"

    args = run.call_args.args[0]
    kwargs = run.call_args.kwargs
    assert _TITLE not in args[-1]
    assert _INITIALDIR not in args[-1]
    assert kwargs["env"]["PYSUS_DIALOG_TITLE"] == _TITLE
    assert kwargs["env"]["PYSUS_DIALOG_INITIALDIR"] == _INITIALDIR


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="Linux")
def test_linux_values_are_passed_as_arguments(_, run):
    run.return_value = Mock(stdout="/tmp/selected\n")

    assert native_dir_picker(_TITLE, _INITIALDIR) == "/tmp/selected"

    args = run.call_args.args[0]
    assert args[-1] == f"--title={_TITLE}"
    assert args[-2] == f"--filename={_INITIALDIR}/"


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="Linux")
def test_linux_falls_back_to_kdialog_when_zenity_is_unavailable(_, run):
    run.side_effect = [
        FileNotFoundError,
        Mock(stdout="/tmp/selected-by-kdialog\n"),
    ]

    assert native_dir_picker(_TITLE, _INITIALDIR) == "/tmp/selected-by-kdialog"

    assert run.call_count == 2
    assert run.call_args_list[1].args[0] == [
        "kdialog",
        "--getexistingdirectory",
        _INITIALDIR,
        "--title",
        _TITLE,
    ]


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="Linux")
def test_linux_returns_empty_when_all_pickers_fail(_, run):
    run.side_effect = [
        FileNotFoundError,
        subprocess.TimeoutExpired("kdialog", 30),
    ]

    assert native_dir_picker(_TITLE, _INITIALDIR) == ""
    assert run.call_count == 2


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="Windows")
def test_cancelled_picker_returns_empty(_, run):
    run.return_value = Mock(stdout="")

    assert native_dir_picker(_TITLE, _INITIALDIR) == ""


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="FreeBSD")
def test_unsupported_platform_returns_empty_without_running_command(_, run):
    assert native_dir_picker(_TITLE, _INITIALDIR) == ""
    run.assert_not_called()
