from dataclasses import dataclass


@dataclass(frozen=True)
class DownloadActionAvailability:
    add: bool
    remove: bool
    clear: bool
    download: bool


def download_action_availability(
    selected_result_count: int,
    selected_queue_count: int,
    queue_size: int,
) -> DownloadActionAvailability:
    """Return which download-queue actions are currently available."""
    has_queue = queue_size > 0
    return DownloadActionAvailability(
        add=selected_result_count > 0,
        remove=selected_queue_count > 0,
        clear=has_queue,
        download=has_queue,
    )
