# Flowgrid

> AI was used to create a significant portion on this project.

Flowgrid is a desktop productivity and workflow tool for repair operations. 
It combines a quick-launch input grid, a tracker hub for repair activity, inventory/parts coordination, admin maintenance, and dashboard reporting for a repair process.

> Please put the assets folder, flowgrid.pyw, and flowgrid_installer.pyw into a shared location to establish your database and home for this project.
> Using the installer will download the assets and main Flowgrid.pyw into your documents folder creating a config file to save any customizations.

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
* Launch buttons for the Agent window, QA/WCS window, workbook import, Admin Panel, and Data Dashboard 

#### Agent Window

The Agent window is where technicians log day-to-day repair activity and track part-related follow-up. 
* The Work tab records work order status updates
* Parts and Client tabs help agents review their own requested part updates, aging, notes, and items that attention.
* 'Parts' lists out any parts needing installed by the requestor, 'Cat. Parts' (or category parts) lists out any parts needing work for the entire category, requestor agnostic (Tech 3 and admins can see the requestor).

#### QA/WCS Window

The QA/WCS window manages part submissions and fulfillment tracking. 
* Users submit parts (preferably, when all parts are delivered) against a work order.
* QA/WCS can apply flags and comments for follow ups
* Bulk import delivered parts by simply copy pasting rows when needed ("delivered" parts will be submitted)
* Monitor progress through the Assigned Parts, Parts Delivered, and Missing PO follow-up views.

#### Admin Panel

The Admin Panel maintains user access, reference data and access-controlled records. 
* Administrators use it to add, update, or remove admins/agents.
* Add/modify QA flags so the operational windows always draw from a current centralized list.
* Admins are tracked separatly.

#### Data Dashboard

The Data Dashboard is the reporting and inspection window for shared tracker data. 
* Administrators can view the table/list.
* The table will show a bargraph of the productivity window selected.
* The list will filter down the specific units that have been submitted.
* Both utilize the same filter by timeframe/user.
* The Completed tab is the canonical completed-parts review surface.
* Use the Notes tab to inspect and edit supported note fields directly on existing records.
* Export results to CSV.

### Settings

Settings controls application behavior and appearance. 
