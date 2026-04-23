# Flowgrid Python File Guide

This guide gives a simple description of what each Python file in Flowgrid does. Some files are full feature modules, and some are small wrapper files that point to the bigger modules behind them.

## Root Files

### `Flowgrid.pyw`
This is the tiny file that starts Flowgrid. It does almost no work by itself and simply hands control to the main startup code in `flowgrid_app/bootstrap.py`.

### `Flowgrid_installer.pyw`
This is the standalone installer and updater for Flowgrid. It runs from the shared drive, checks Python and package requirements, downloads the runtime from GitHub `main`, writes local install metadata, overlays shared Assets into the local runtime, creates shortcuts, and can relaunch the app after updates.

## `flowgrid_app`

### `flowgrid_app/__init__.py`
This is the package front door for the app. It gathers a few important classes and services into one place so other files can import them more easily.

### `flowgrid_app/app_context.py`
This file defines small data containers used while the app is running. They hold shared things like runtime options, config data, user services, and references to active windows.

### `flowgrid_app/auth_permissions.py`
This file is the main rules checker for access control. It defines role snapshots, permission errors, and the logic that decides what each user is allowed to open or use.

### `flowgrid_app/bootstrap.py`
This is the main startup manager for the app. It checks Python and package requirements, validates important paths, turns on logging, handles startup errors, supports command-line modes, and launches the Qt interface.

### `flowgrid_app/depot_db.py`
This is a small wrapper file. It re-exports the `DepotDB` class from `workflow_core.py` so the rest of the app can import it from a simpler location.

### `flowgrid_app/depot_refresh.py`
This is another small wrapper file. It re-exports the refresh coordinator classes from `workflow_core.py` so refresh logic can be imported without reaching into the larger core file directly.

### `flowgrid_app/depot_rules.py`
This file holds shared business rules and option lists for the depot side of the app. It defines things like role labels, category options, follow-up actions, and other central rule values used across windows.

### `flowgrid_app/depot_schema.py`
This is a thin wrapper around the schema manager in `workflow_core.py`. Its job is to expose the `DepotSchema` class from the main core module.

### `flowgrid_app/depot_tracker.py`
This file is a thin wrapper around the tracker logic in `workflow_core.py`. It exposes the main tracker class and a few shared tracker constants used by other parts of the app.

### `flowgrid_app/diagnostics.py`
This file runs health checks for the app and installer. It inspects the database, permissions, shortcut setup, and UI startup behavior so problems can be found and reported clearly.

### `flowgrid_app/icon_io.py`
This file handles icon image work. It loads icon files, cleans up transparency, converts image data, builds smoother app icons, and writes icon files used for shortcuts and the UI.

### `flowgrid_app/installer.py`
This file contains shared installer helper logic. It focuses on local install tasks like shortcut creation, finding the right Python executable, and launching the app after setup.

### `flowgrid_app/legacy_runtime.py`
This is the old giant all-in-one version of much of Flowgrid. It appears to keep older runtime code in one place, likely for legacy support, fallback behavior, or comparing old behavior to the newer split-up modules.

### `flowgrid_app/paths.py`
This file is the central path and file-location manager. It figures out where shared data, local config files, the local standalone installer, install-state metadata, databases, icons, and migrated older files should live.

### `flowgrid_app/runtime_logging.py`
This file is the main runtime logging system. It figures out where logs should go, writes log entries, rotates old logs, and records useful user and machine details for troubleshooting.

### `flowgrid_app/update_manager.py`
This file contains the app-side update and shared-asset sync logic. It reads and writes the install-state manifest, checks GitHub `main` for new commits, downloads and stages snapshots when needed, and syncs shared Assets into the local runtime.

### `flowgrid_app/ui_utils.py`
This file holds small helper functions for the UI. Most of them deal with safe number handling, color math, contrast checks, and building theme palettes.

### `flowgrid_app/user_repository.py`
This file manages user and role data. It reads and updates users, roles, agents, and admin records while also answering questions about access levels and display information.

### `flowgrid_app/window_manager.py`
This file keeps track of open windows and dialogs. It helps the app reuse existing windows, avoid duplicates, and close managed windows in a controlled way.

### `flowgrid_app/workflow_core.py`
This is one of the most important files in the project. It contains the main database access layer, schema updates, refresh tracking, and business logic for work orders, parts, notes, flags, dashboard data, and many other depot actions.

## `flowgrid_app/window`

### `flowgrid_app/window/__init__.py`
This file marks the window folder as a Python package. It also acts as a simple label showing that this folder contains the standalone window modules for Flowgrid tools.

### `flowgrid_app/window/admin.py`
This is a tiny wrapper file for the admin dialog. It re-exports the `DepotAdminDialog` class from `operations.py`.

### `flowgrid_app/window/agent.py`
This file builds the Agent window used by technicians. It handles daily work logging, parts views, alerts, notes, install actions, and the other tools an agent uses during normal repair work.

### `flowgrid_app/window/common.py`
This file holds shared window helpers used by more than one screen. It includes date formatting helpers, note preview helpers, and reusable widgets like alert tabs and touch charts.

### `flowgrid_app/window/constants.py`
This file stores a few shared window timing values. These constants help keep refresh timing, debounce timing, and similar UI behavior consistent across windows.

### `flowgrid_app/window/dashboard.py`
This is a small wrapper for dashboard classes. It re-exports the dashboard chart and dashboard dialog from `operations.py`.

### `flowgrid_app/window/icon_tools.py`
This is a small wrapper for icon editing tools. It re-exports the icon crop and arrangement classes from `operations.py`.

### `flowgrid_app/window/operations.py`
This file contains several larger shared operation windows and tools. It includes the admin dialog, dashboard dialog, dashboard chart, and image/icon editing pieces used elsewhere in the app.

### `flowgrid_app/window/popup_support.py`
This file provides the shared popup and dialog framework for Flowgrid. It defines the themed dialog classes and helper functions used for messages, file pickers, color pickers, input boxes, and frameless tool windows.

### `flowgrid_app/window/qa_qcs.py`
This file builds the QA/WCS window. It handles part submission, bulk part entry, assigned and delivered part views, category part review, client follow-up work, and other QA-focused tasks.

### `flowgrid_app/window/query_support.py`
This file contains helper code for query-related tasks. It builds small SQL pieces and handles the parsing, cleanup, and storage of part-detail text and alert quiet-time values.

### `flowgrid_app/window/quick_designer.py`
This file powers the quick-input layout and design tools. It manages quick button cards, layout editing, radial menus, image layers, background previews, and the tools used to customize that part of the app.

### `flowgrid_app/window/shared_actions.py`
This file holds shared actions used by more than one depot window. It includes popups and helper actions for part notes, missing purchase-order follow-ups, and common table-based actions.

### `flowgrid_app/window/shell.py`
This is the main Flowgrid application window. It builds the overall shell, navigation, quick-input pages, settings, themes, and the logic that opens and refreshes the other major tool windows.

### `flowgrid_app/window/table_support.py`
This file contains shared table helpers for the UI. It standardizes table setup, centered icons and text, row selection behavior, and a few common table actions like copying work orders.
