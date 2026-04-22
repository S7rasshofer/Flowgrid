---
name: reporting-calculation-authority
description: Enforces centralized, reusable, and deterministic dashboard and summary metric logic for the Flowgrid Python application.
argument-hint: Provide a Python reporting task, metric definition, grouped summary issue, aging calculation, or dashboard rollup problem.
---

# Supporting Skills for: Flowgrid Data Visualizer

These skills reinforce stable reporting, centralized calculations, and dashboard consistency inside the Flowgrid Python desktop application.

---

## 1. Reporting Calculation Authority

**Purpose**  
Owns reusable summary and metric logic so dashboard math is not duplicated across tabs, dialogs, or widgets.

**Use When**
- Adding new summary cards
- Creating grouped counts
- Building aging buckets
- Reusing the same metric in multiple places

**Enforces**
- One authoritative calculation per metric
- Deterministic rollup logic
- Explicit assumptions for statuses, date ranges, and filters
- No copy-pasted query math in UI code

---

## 2. Chart Data Shaping Guard

**Purpose**  
Ensures raw application data is transformed into chart-ready structures in a consistent and testable way.

**Use When**
- Building chart tabs
- Adding trend lines
- Creating bar, pie, or aging visualizations
- Preparing grouped datasets for admin views

**Enforces**
- Stable label/value shaping
- Predictable ordering
- Consistent empty-state handling
- No chart-specific data massaging buried inside widgets

---

## 3. Dashboard Consistency Enforcer

**Purpose**  
Keeps summary panels, dashboard tabs, and reporting windows behaviorally consistent.

**Use When**
- Adding dashboard sections
- Updating admin summary tabs
- Standardizing status displays
- Making multiple reporting views align

**Enforces**
- Shared terminology
- Shared filtering behavior
- Consistent section structure and spacing
- Matching treatment for loading, empty, stale, and error states

---

## 4. Reporting Query Safety Guard

**Purpose**  
Protects reporting code from fragile or misleading queries.

**Use When**
- Writing aggregation queries
- Adding grouped filters
- Investigating incorrect counts
- Debugging mismatched dashboard totals

**Enforces**
- Explicit filter boundaries
- Clear distinction between no data and query failure
- Safe date/status filtering
- Logging for failed or suspicious reporting queries

---

## 5. Stale Data Detection Bridge

**Purpose**  
Helps the visualizer distinguish between a reporting bug and a data freshness bug.

**Use When**
- Dashboard does not reflect recent updates
- Admin summaries lag behind user actions
- Counts differ between windows
- Trend views seem incorrect after new submissions

**Enforces**
- Freshness checks before blaming chart logic
- Explicit “data may be stale” reasoning
- Escalation to `data-guardian` when the root issue is sync/path/shared-state related

---

# Recommended Minimum Set

If you want the leanest useful support layer, start with:

1. Reporting Calculation Authority  
2. Chart Data Shaping Guard  
3. Reporting Query Safety Guard  

That gives the visualizer a strong base without overcomplicating the system.

---

# Best Fit with Your Current Agent Setup

- `flowgrid-app-guardian`  
  Parent governor for scope, consistency, and safe change control

- `data-guardian`  
  Shared-drive, installer/runtime, SQLite, and sync authority

- `flowgrid-data-visualizer`  
  Dashboard, summaries, rollups, and chart-facing logic

- Supporting skills for the visualizer  
  Keep reporting logic centralized, safe, and consistent

---

# Design Principle

The visualizer agent should answer:
- What should this dashboard show?
- How should this metric be derived?
- Where should this calculation live?

Its supporting skills should answer:
- Is the calculation reusable?
- Is the chart input shaped correctly?
- Is the query safe?
- Is this really a reporting issue, or stale shared data?

---