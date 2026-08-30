from unittest.mock import Mock, patch

from pysus.native_dir_picker import native_dir_picker

_TITLE = "Select'; Write-Output injected; '"
_INITIALDIR = 'C:\\data" & do shell script "whoami'


@patch("pysus.native_dir_picker.subprocess.run")
@patch("pysus.native_dir_picker.platform.system", return_value="Windows")
def test_windows_values_are_not_interpolated_into_script(_, run):
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
def test_macos_values_are_not_interpolated_into_script(_, run):
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
