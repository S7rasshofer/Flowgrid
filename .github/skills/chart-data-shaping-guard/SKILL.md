---
name: chart-data-shaping-guard
description: Enforces stable transformation of Flowgrid operational data into chart-ready structures for dashboard and admin reporting views.
argument-hint: Provide a Python charting task, dashboard tab issue, trend calculation, grouped dataset problem, or visualization data-shaping request.
---

# Chart Data Shaping Guard

## Purpose

Ensures raw application data is transformed into chart-ready structures in a consistent and testable way.

## Use When

- Building chart tabs
- Adding trend lines
- Creating bar, pie, or aging visualizations
- Preparing grouped datasets for admin views

## Enforces

- Stable label/value shaping
- Predictable ordering
- Consistent empty-state handling
- No chart-specific data massaging buried inside widgets

## Escalation Rules

Escalate to `flowgrid-data-visualizer` when chart-ready datasets, trend preparation, or reusable dashboard shaping logic must be designed or corrected.

Escalate to `data-guardian` if chart problems are caused by stale data, wrong database paths, sync delays, or shared-state inconsistencies.

Escalate to `flowgrid-app-guardian` if chart changes risk widening into broader UI or architectural changes.