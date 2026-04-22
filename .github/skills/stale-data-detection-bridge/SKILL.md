---
name: stale-data-detection-bridge
description: Helps distinguish Flowgrid dashboard/reporting issues from shared-data freshness, sync, or path-resolution problems.
argument-hint: Provide a Python dashboard freshness issue, stale summary problem, delayed update report, or mismatch between user actions and displayed metrics.
---

# Stale Data Detection Bridge

## Purpose

Helps the visualizer distinguish between a reporting bug and a data freshness bug.

## Use When

- Dashboard does not reflect recent updates
- Admin summaries lag behind user actions
- Counts differ between windows
- Trend views seem incorrect after new submissions

## Enforces

- Freshness checks before blaming chart logic
- Explicit “data may be stale” reasoning
- Escalation to `data-guardian` when the root issue is sync, path, or shared-state related

## Escalation Rules

If the active issue is primarily outside this skill’s domain, do not force a solution through this skill.

Escalate to the appropriate agent when:
- the root cause belongs to installer/runtime/shared-data behavior
- the root cause belongs to dashboard/reporting aggregation behavior
- the issue is application-wide and needs parent-agent review

This skill should constrain the active agent’s work, not replace agent-level scope decisions.