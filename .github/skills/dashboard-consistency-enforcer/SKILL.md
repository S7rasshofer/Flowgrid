---
name: dashboard-consistency-enforcer
description: Enforces consistent behavior, structure, terminology, and state handling across Flowgrid dashboard panels, reporting tabs, and admin summary views.
argument-hint: Provide a Python dashboard UI task, summary tab issue, reporting window inconsistency, or admin panel behavior question.
---

# Dashboard Consistency Enforcer

## Purpose

Keeps summary panels, dashboard tabs, and reporting windows behaviorally consistent.

## Use When

- Adding dashboard sections
- Updating admin summary tabs
- Standardizing status displays
- Making multiple reporting views align

## Enforces

- Shared terminology
- Shared filtering behavior
- Consistent section structure and spacing
- Matching treatment for loading, empty, stale, and error states

## Escalation Rules

Escalate to `flowgrid-data-visualizer` when dashboard sections, summary views, admin tabs, or reporting surfaces need coordinated consistency.

Escalate to `flowgrid-app-guardian` when the issue extends beyond reporting and affects wider Flowgrid UI standards, shared window behavior, or application-wide consistency.

Escalate to `data-guardian` only if inconsistency is caused by stale shared data rather than dashboard structure or behavior.