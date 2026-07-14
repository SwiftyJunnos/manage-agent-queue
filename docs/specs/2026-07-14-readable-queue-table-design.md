# Readable Queue Table Design

**Date:** 2026-07-14

**Status:** Approved

**Approved artifact:** Readable Light Table v3

## Goal

Make the live queue easy to scan without turning it into a dashboard of cards or a decorative timeline. The first view should answer two questions quickly:

1. Which task is active now?
2. What is the overall queue progress?

The interface remains a read-only local queue observer. It does not add queue mutation controls or change server data contracts.

## Information hierarchy

The queue view uses this order:

1. Queue name, revision, connection state, last successful refresh, and manual refresh.
2. One compact summary line containing completed count, active count, waiting count, attention count, and a thin progress indicator.
3. One task table per workflow group, including the existing `unassigned` group.
4. The Activity view remains available as a secondary tab.

Summary metrics must not be rendered as separate cards. They support the task table instead of competing with it.

## Queue table

Use a semantic table with these visible columns:

| Column | Content |
| --- | --- |
| Status | Text and a simple state marker, such as `Active` or `Waiting` |
| Task | Two-line task presentation described below |
| Assignee | Agent ID or `Unassigned` |
| Timing | Remaining lease time when relevant, otherwise an em dash |

Each task occupies one table row. The Task cell uses two visual lines:

- Primary line: task ID followed by the task title.
- Secondary line: attempts, dependency, and resources as quiet labeled metadata.

Task title, assignee, timing, and metadata must not run together as one sentence. Long Korean titles wrap within the Task column without colliding with adjacent columns.

The active row receives only two additional treatments: a very light green background and a 3-pixel green leading edge. Other rows rely on separators and typography rather than individual containers.

## Visual language

The approved interface is light mode:

- pale neutral page background;
- white table surface;
- thin gray header and row separators;
- dark neutral primary text;
- medium gray secondary text;
- restrained blue for task IDs and the selected tab;
- semantic green for the live state and active row;
- no gradients, shadows on rows, decorative timeline nodes, or repeated rounded cards.

Spacing and type weight establish hierarchy. Color is supplementary and never the only state indicator.

## Responsive behavior

At desktop widths, retain the four-column table.

Below 760 pixels:

- visually hide the table header while preserving its accessible structure;
- present each row as a compact two-column block;
- keep Status in the leading column;
- stack Task, Assignee, and Timing in the content column;
- preserve the two-line Task hierarchy and active-row leading edge;
- allow titles, assignees, dependencies, and resources to wrap without horizontal scrolling.

## Data and behavior

Reuse the existing snapshot and event APIs without schema changes. Build rows from `workflows[].tasks` in their existing order. Render one grouped table for each workflow projection.

Preserve current behavior:

- automatic revision polling;
- incremental Activity events;
- manual refresh;
- last successful refresh time;
- retrying and stopped connection states;
- changed-task indication;
- read-only operation and existing security boundaries.

The redesign may replace expandable task rows. All currently visible task metadata remains visible in the two-line row, so expanding a row is not required for the core queue view.

## States

- **Loading:** show the table structure or a restrained loading message without fabricated task rows.
- **Empty:** replace the grouped tables with the existing empty-state message.
- **Live:** show the green text-and-dot connection indicator.
- **Retrying:** keep the most recent table visible and change only the connection message to the warning state.
- **Stopped:** keep the most recent table visible, show the stopped explanation, and stop polling.
- **Attention:** use the existing warning information above the affected group or row without introducing a separate card grid.
- **Completed queue:** show the completed count and full progress indicator while retaining completed task rows.

## Accessibility

- Use semantic `table`, `thead`, `tbody`, `th`, and `td` elements.
- Keep status text in addition to shape and color.
- Preserve keyboard-operable tabs and refresh control with visible focus styles.
- Maintain readable contrast for primary and secondary text in light mode.
- Do not communicate changes through motion alone; honor reduced-motion preferences for any changed-row highlight.
- Keep connection changes in the existing polite live region.

## Verification

Add or update tests that verify:

- semantic table markup and the four column labels;
- two-line task content with separate primary and secondary metadata;
- absence of the five-card summary grid and workflow card layout;
- active-row class and text status;
- light color scheme and required responsive breakpoint;
- safe DOM construction without `innerHTML` or external assets;
- loading, empty, live, retrying, stopped, attention, and completed rendering contracts;
- existing polling, activity, security, and lifecycle tests continue to pass.

Manually inspect realistic Korean task titles at desktop and sub-760-pixel widths. Confirm wrapping, column alignment, keyboard focus, Activity tab switching, and stale-data visibility during retrying and stopped states.

## Scope

This change is limited to the checked-in dashboard HTML, CSS, JavaScript, their tests, and documentation that describes the browser view. It adds no dependency, build step, mutation endpoint, queue schema change, or server projection change.
