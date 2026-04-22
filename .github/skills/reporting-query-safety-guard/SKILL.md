---
name: reporting-query-safety-guard
description: Protects Flowgrid reporting code from fragile, misleading, or inconsistent aggregation and filter queries.
argument-hint: Provide a Python reporting query, grouped filter issue, incorrect count problem, date filter bug, or dashboard aggregation task.
---

# Reporting Query Safety Guard

## Purpose

Protects reporting code from fragile or misleading queries.

## Use When

- Writing aggregation queries
- Adding grouped filters
- Investigating incorrect counts
- Debugging mismatched dashboard totals

## Enforces

- Explicit filter boundaries
- Clear distinction between no data and query failure
- Safe date/status filtering
- Logging for failed or suspicious reporting queries