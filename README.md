# Flowgrid

A professional-grade Python desktop application for repair team productivity tracking, parts logging, inventory management, and operational dashboards. Built with PySide6 (Qt) for a modern, themeable UI with real-time team reporting.

## Features

### Core Functionality
- **Agent Window** – Task submission and progress tracking for individual team members
- **QA Window** – Quality assurance review and approval workflows
- **Admin Panel** – User and permission management (restricted to classified admins)
- **Dashboard** – Real-time team productivity metrics and operational status (admin-only)
- **Parts Tracking** – Log, flag, and manage inventory with aging indicators
- **Productivity Monitoring** – Centralized team metrics and reporting
- **Operational Status Flags** – Custom status indicators for workflow management

### Data Architecture
- **Centralized Shared Database** – All production data lives on shared drive (`Z:\DATA\Flowgrid`)
- **Local Queuing** – User submissions queue locally and sync with shared database
- **Network-Safe Operations** – Optimized for team environments with minimal latency
- **Automatic Path Resolution** – Configurable data locations via `Flowgrid_paths.json`

### User Interface
- **Multiple Themed Variants** – Separate visual modes for Agent, QA, Admin, and main window
- **10+ Built-in Themes** – Default, Classic, Slate, Forest, Ocean, Midnight, Desert, Sage, Crimson, Steel, Amber
- **Themeable Controls** – Customizable colors, opacity, hover delays, and layout
- **Role-Based Access** – Hardcoded super-admin privileges; permission checks on all restricted features
- **Desktop Integration** – Native Windows desktop shortcuts and auto-launch support

### Security & Access Control
- **Permission-Based UI** – Admin and dashboard panels restricted to classified users only
- **Hardcoded Super-Admin** – Unlisted super-admin access for system management
- **User Normalization** – Consistent username handling and validation
- **Centralized Permission Logic** – All permission checks routed through `DepotTracker`

## Installation

### Prerequisites
- Python 3.14+
- Windows OS (native path handling and desktop integration)
- PySide6 (auto-installed by installer)

### Quick Start
1. Download the latest release
2. Run `Flowgrid_installer.pyw` from the shared drive
3. Installer will:
   - Verify Python environment
   - Install dependencies (PySide6, etc.)
   - Initialize local database
   - Create desktop shortcut
   - Sync with shared drive

### Manual Setup
```bash
pip install PySide6
python Flowgrid.pyw
```

## Configuration

### Path Configuration
Edit `Flowgrid_paths.json` to customize data locations:

```json
{
  "shared_drive_root": "Z:\\DATA\\Flowgrid",
  "local_paths": {
    "database_folder": "{DOCUMENTS}\\Flowgrid\\Data",
    "queue_folder": "{DOCUMENTS}\\Flowgrid\\Queue",
    "config_folder": "{DOCUMENTS}\\Flowgrid\\Config",
    "assets_folder": "{DOCUMENTS}\\Flowgrid\\Assets"
  }
}
```

### Theme Configuration
Edit `Flowgrid_config.json` to customize appearance:

```json
{
  "theme": {
    "primary": "#C35A00",
    "accent": "#FF9A1F",
    "surface": "#090A0F"
  },
  "selected_theme_preset": "Default",
  "window_opacity": 1.0,
  "compact_mode": true
}
```

See [PATHS_CONFIGURATION_GUIDE.md](PATHS_CONFIGURATION_GUIDE.md) and [QUICKSTART_PATHS_CONFIG.md](QUICKSTART_PATHS_CONFIG.md) for detailed configuration instructions.

## Project Structure

```
z:\DATA\Flowgrid/
├── Flowgrid.pyw                      # Main application
├── Flowgrid_installer.pyw            # Installation script
├── Flowgrid_config.json              # Theme and UI settings
├── Flowgrid_paths.json               # Path configuration
├── Flowgrid_depot.db                 # Shared production database (SQLite)
├── Assets/                           # UI icons and images
│   ├── admin_icons/
│   ├── agent_icons/
│   ├── qa_flag_icons/
│   ├── part_flag_images/
│   ├── ui_icons/
│   └── Flowgrid Icons/
├── README.md                         # This file
├── AGENTS.md                         # Developer guidelines
└── PATHS_CONFIGURATION_GUIDE.md      # Configuration reference
```

## Architecture

### Database
- **Location:** Shared drive (`Z:\DATA\Flowgrid\Flowgrid_depot.db`)
- **Type:** SQLite3
- **Tables:** users, submissions, parts, admin_users, status flags
- **Access:** All reads and writes from shared location; local queuing for submissions

### Application Structure
- **DepotTracker** – Central business logic and database operations
- **DepotAgentWindow** – Agent task submission UI
- **DepotQAWindow** – QA approval workflows
- **DepotAdminDialog** – Admin user and permission management
- **DepotDashboardDialog** – Real-time team metrics and reporting
- **QuickInputsWindow** – Main window with grid-based task view
- **DepotFramelessToolWindow** – Base class for themed tool windows

### Logging
- **Launch Errors:** `Flowgrid_launch_errors.log`
- **Runtime Events:** `Flowgrid_runtime_*.log.jsonl` (rotating, 10MB per file, 20 backups)
- **All failures logged** – No silent errors; actionable diagnostics for support

## Permissions & Access Control

### Admin Panel Access
- Restricted to users in `admin_users` table
- Hardcoded super-admin ('KIDDS') bypasses table lookup
- Permission check on dialog open; unauthorized users denied access

### Dashboard Access
- Restricted to classified admin users only
- Same permission logic as admin panel
- Real-time team metrics visible only to authorized personnel

## Usage

### Running the Application
```bash
python Flowgrid.pyw
```

### Agent Workflow
1. Open Agent Window (role-based UI)
2. Select part or task
3. Submit via Quick Inputs
4. Status tracked in Dashboard (admins only)

### Admin Workflow
1. Open Admin Panel (permission-restricted)
2. Manage users, permissions, and status flags
3. Changes sync to shared database
4. Review team metrics in Dashboard

## Development

### Guidelines
- See [AGENTS.md](AGENTS.md) for architecture principles and stability mode
- Keep business logic in `DepotTracker`; UI in window classes
- Centralize permission checks; no silent failures
- Prefer shared components over duplicating window logic
- All new windows must follow themeable window pattern

### Testing
- Verify database paths resolve correctly before launch
- Test admin panel access with multiple user roles
- Validate theme changes persist across restarts
- Confirm queue sync operations complete without errors

## Support

For issues, configuration problems, or feature requests, check:
- [PATHS_CONFIGURATION_GUIDE.md](PATHS_CONFIGURATION_GUIDE.md)
- [QUICKSTART_PATHS_CONFIG.md](QUICKSTART_PATHS_CONFIG.md)
- [AGENTS.md](AGENTS.md) for architecture and stability guidelines
- Runtime logs in `Flowgrid_runtime_*.log.jsonl` for detailed diagnostics

## License

Proprietary – Repair Team Productivity System

---

**Flowgrid** – Built for teams. Designed for stability. Ready for scale.
