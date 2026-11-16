from pysus.download_queue import download_action_availability


def test_actions_are_disabled_without_selections_or_queue():
    actions = download_action_availability(0, 0, 0)

    assert not actions.add
    assert not actions.remove
    assert not actions.clear
    assert not actions.download


def test_result_selection_enables_only_add_for_empty_queue():
    actions = download_action_availability(2, 0, 0)

    assert actions.add
    assert not actions.remove
    assert not actions.clear
    assert not actions.download


def test_queue_enables_clear_and_download_without_selection():
    actions = download_action_availability(0, 0, 3)

    assert not actions.add
    assert not actions.remove
    assert actions.clear
    assert actions.download


def test_selected_queue_item_enables_removal():
    actions = download_action_availability(0, 1, 3)

    assert not actions.add
    assert actions.remove
    assert actions.clear
    assert actions.download
