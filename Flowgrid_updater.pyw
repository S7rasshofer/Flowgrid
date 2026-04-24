"""Thin Flowgrid updater launcher."""

from flowgrid_app.update_manager import _parse_updater_cli_options, run_updater_mode


if __name__ == "__main__":
    options = _parse_updater_cli_options()
    raise SystemExit(
        run_updater_mode(
            launch_after_update=bool(options.get("launch_after_update", True)),
            relaunch_after_update=bool(options.get("relaunch_after_update", False)),
            launch_on_failure=bool(options.get("launch_on_failure", False)),
            parent_pid=int(options.get("parent_pid", 0) or 0),
        )
    )
