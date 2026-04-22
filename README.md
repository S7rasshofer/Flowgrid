# Flowgrid

> AI was used to create a significant portion on this project.

Flowgrid is a desktop productivity and workflow tool for repair operations. 
It combines a quick-launch input grid, a tracker hub for repair activity, inventory/parts coordination, admin maintenance, and dashboard reporting for a repair process.

> Put `Assets`, `Flowgrid.pyw`, `Flowgrid_installer.pyw`, and `flowgrid_app` in the shared Flowgrid source location.
> Run `Flowgrid_installer.pyw` from the shared source to install the local runtime in `Documents\\Flowgrid`.
> `python Flowgrid.pyw --create-shortcut` creates or repairs the local desktop shortcut.
> `python Flowgrid.pyw --install` remains a legacy alias for local shortcut setup.
> `python Flowgrid.pyw --diagnose-install` runs a read-only install preflight.
> `python Flowgrid.pyw --smoke-ui` runs the offscreen UI smoke harness against the shared DB in read-only mode.

## Main Window

The main Flowgrid window is the launcher and navigation shell. 
Users move between the Input Grid, Tracker Hub, and Settings from the sidebar, while the title bar and shared theme keep the overall experience consistent across the app.

### Input Grid

The Input Grid is a highly customizable workspace for quick buttons speeding up repetitive tasks. 
* Users create or edit buttons.
    * Configure each button to paste text, open links, launch apps or files, or run simple input sequences.
* Organize the layout and separate into tabs for different job tasks.

### Tracker Hub

Tracker Hub is the operational home page for workflow tools. 
* Launch buttons for the Workflow windows and reporting.
    * Agent, QA/WCS, User Setup, and Data Dashboard.

#### Agent 

The Agent window is where technicians log day-to-day repair activity and track part-related follow-up. 
* The Work tab records work order status updates
* Parts and Client tabs help agents review their own requested part updates, aging, notes, and items that attention.
* 'Parts' lists out any parts needing installed by the requestor, 'Cat. Parts' (or category parts) lists out any parts needing work for the entire category, requestor agnostic (Tech 3 and admins can see the requestor).

#### QA/WCS 

The QA/WCS window manages part submissions and fulfillment tracking. 
* Users submit parts (preferably, when all parts are delivered) against a work order.
* QA/WCS can apply flags and comments for follow ups
* Bulk import delivered parts by simply copy pasting rows when needed ("delivered" parts will be submitted)
* Monitor progress through the Assigned Parts, Parts Delivered, and Completed tabs.
* If a part order is missed and a QA submits parts this will be logged in a 'Missing PO' follow-up view for elevated reporting/admin access.

#### User Setup

The User Setup window maintains user access, reference data and access-controlled records. 
* Administrators use it to add, update, or remove users from a merged user list.
* Users can be assigned one operational Role plus optional Access for reporting or full administration.
* Administrators can maintain the centralized Roles list that drives the Role dropdown in User Setup.
* Add/modify QA flags so the operational windows always draw from a current centralized list.

#### Data Dashboard

The Data Dashboard is the reporting and inspection window for shared tracker data. 
* Administrators can view the table/list.
* The table will show a bargraph of the productivity window selected.
* The list will filter down the specific units that have been submitted.
* Both utilize the same filter by timeframe/user.
* Use the Notes tab to inspect and edit supported note fields directly on existing records.
* Export results to CSV.

### Settings

Settings controls application behavior and appearance. 
