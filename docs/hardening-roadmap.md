# Hardening Roadmap

## Session 1: Request Assembly and Input Validation

Scope: harden the query-building path in `GenesysConvAnalyzer.ps1` so invalid UI inputs fail early and predictably.

Task: add explicit region validation before building `https://api.<region>`.

Task: add stricter validation for start/end date-time combinations and preserve the exact interval handed to the request preview and submit path.

Task: centralize request assembly so preview and submit always use the same endpoint/body builder.

Task: replace any remaining script-local assumptions about control values with resolved defaults.

Validation: run a PowerShell parse check on the script; manually validate preview and submit behavior for valid region, invalid region, blank start time, blank end time, end-before-start, and preset-based intervals; confirm the preview endpoint/body exactly match what is logged before submit.

Documentation: update any usage notes or inline help in the script to describe actual date-time defaults, region validation behavior, and request preview behavior as implemented.

## Session 2: API Resilience and Rate-Limit Handling

Scope: harden the direct REST path in the WPF app around `Invoke-GcApiRequest`.

Current implementation status:

- `GenesysConvAnalyzer.ps1` now routes GUI API calls through `src/ui/UiApiRetry.ps1`.
- Retry/backoff is implemented for transient `429` and `5xx` failures on `GET`, `HEAD`, `OPTIONS`, and `DELETE`.
- `Retry-After` is honored when present and each scheduled retry is written to the job log.
- `POST` job submission is intentionally not retried automatically to avoid duplicate async jobs.
- Cancel/delete failures are now logged as warnings and surfaced in the UI instead of being swallowed.
- Local validation entry point: `powershell.exe -File scripts/Test-UiApiRetry.ps1`

Task: add retry/backoff handling for transient HTTP failures, especially 429 and 5xx responses.

Task: honor retry delay headers when present and surface retry attempts in the UI log.

Task: separate fatal request failures from retriable failures so the user sees accurate status.

Task: stop swallowing delete/cancel failures silently and log them as warnings.

Validation: add a request-invoker seam or stub path so retry logic can be exercised locally; run targeted tests for success, single retry then success, repeated 429, repeated 500, and cancel failure logging; verify parse check still passes.

Documentation: update repo documentation and inline comments to describe the implemented retry policy, what is retried, what is not, and what users should expect in the job log.

## Session 3: Polling and Paging Guardrails

Scope: prevent runaway polling and result collection in the job monitor and collection flow.

Task: add max-poll count, max-consecutive-poll-error count, and timeout behavior for job monitoring.

Task: add cursor loop detection and max-page count to result collection.

Task: fail collection cleanly when the API repeats a cursor or returns malformed paging state.

Task: surface these stop conditions clearly in the UI and log.

Validation: run targeted tests with mocked or simulated polling states and cursor responses to confirm stop conditions trigger correctly; manually verify success flow still works for a normal multi-page run.

Documentation: update documentation and inline help to describe polling limits, paging safeguards, and the user-visible failure states that now exist.

## Session 4: Large-Data UI Memory Hardening

Scope: reduce predictable memory pressure in the current results workflow.

Task: stop duplicating large result sets where possible between `$script:allConversations` and the `DataTable` used by the grid.

Task: add a conversation lookup index keyed by `conversationId` so row selection does not rescan the full collection.

Task: review result-summary generation and column-refresh paths for unnecessary full-list passes.

Task: add user-facing messaging when a loaded dataset is large enough that UI rendering will be slow.

Validation: run a local load test using a synthetic or repeated JSONL dataset large enough to expose current memory/scanning behavior; confirm row selection no longer performs full collection scans; verify exports and detail view still work.

Documentation: update current-status documentation to describe how results are stored in memory, what indexing exists, and any known dataset-size limits that still remain.

## Session 5: Streamed File IO for Large JSONL Loads

Scope: eliminate avoidable memory spikes in file import/export paths.

Task: replace `ReadAllLines` with streaming reads for JSONL import.

Task: harden JSONL load behavior for blank lines, malformed records, mixed top-level arrays, and partial failures.

Task: make load progress visible for larger files so the UI does not appear hung.

Task: confirm JSONL export paths are consistent with the streamed import expectations.

Validation: run import tests against small JSONL, large JSONL, malformed-line JSONL, and mixed JSON/JSONL files; verify partial-failure behavior is deterministic and clearly reported.

Documentation: update the repo's current usage notes to reflect actual supported file formats, streaming behavior, and known import limitations.

## Session 6: Date/Time Correctness Standardization

Scope: remove remaining fragile `DateTime::Parse` usage and standardize on offset-safe handling.

Task: replace remaining raw `DateTime::Parse` calls in the UI transformation and detail paths with a shared `DateTimeOffset`-safe helper.

Task: normalize display behavior for UTC and local time conversions so summary, detail, and export views are internally consistent.

Task: review the module code and UI code together for any other date parsing paths that can drift or silently reinterpret time zones.

Validation: run targeted tests using timestamps with `Z`, explicit offsets, blank values, and malformed values; verify displayed durations and exported timestamps match expected values.

Documentation: update actual current documentation to state which fields are shown in UTC, which are shown in local time, and how durations are computed.

## Session 7: Structured Telemetry and Failure Visibility

Scope: make the GUI path observable in the same way the module already writes manifests and events.

Task: add structured run-event output for auth, preview, submit, poll, collect, load, and export actions.

Task: convert silent catches and generic error text into explicit warning/error events with actionable context.

Task: add counts for retries, poll attempts, pages collected, duplicates skipped, and load failures.

Task: store telemetry in a durable location that matches actual run behavior, not a planned future layout.

Validation: run through auth, preview, submit, collect, load, and export flows and inspect emitted telemetry files for correctness; verify failed flows also emit structured events.

Documentation: update documentation to describe exactly what telemetry artifacts are written today, where they are stored, and what fields they contain.

## Session 8: WPF App Integration with Genesys.Core

Scope: reduce duplicated logic by moving the WPF app toward the reusable module pipeline under `src/ps-module/Genesys.Core`.

Task: decide the first seam to replace in the GUI with module-backed behavior, starting with acquisition or normalization rather than a full rewrite.

Task: wire the GUI to call module functions for at least one end-to-end path that is currently duplicated.

Task: remove or isolate redundant request/paging logic once the module path is proven.

Task: make the `ModulePath` parameter real or remove it if it is not actually used.

Validation: run side-by-side verification of old vs module-backed behavior for the same query and confirm request shape, collected records, and output artifacts match.

Documentation: update documentation to state exactly which parts of the GUI now depend on `Genesys.Core` and which parts are still script-local.

## Session 9: Automated Test Coverage for Predictable Failures

Scope: add Pester coverage for the failures already observed and the hardening work above.

Task: create a test layout for UI-adjacent pure functions first, especially interval resolution, preset behavior, request assembly, key-safe map handling, and JSONL load parsing.

Task: add module tests for normalization, focus-profile analysis, and any retry/paging helpers touched during hardening.

Task: add fixture data that represents realistic conversation records, large-ish JSONL input, malformed records, and retry/error scenarios.

Task: make test execution straightforward from the repo root and ensure failures are readable.

Validation: run the Pester suite at the end of the session and confirm coverage exists for each hardened feature area changed so far.

Documentation: update documentation to reflect the real test entry points, current fixture coverage, and any known areas that still lack tests.

## Session 10: Codebase Cleanup and Current-State Documentation Pass

Scope: close the loop after the hardening work so the repo does not drift into stale assumptions again.

Task: remove dead code, stale comments, and misleading notes uncovered during earlier sessions.

Task: review inline comments, synopsis text, and exported module metadata for factual accuracy.

Task: add a concise repo-level operational document if one does not exist yet, but only for behavior that is already implemented and verified.

Task: ensure no documentation claims module integration, telemetry, retry policy, or test coverage beyond what is actually in the repo at that point.

Validation: run the script parse check, module import check, and full test suite; manually spot-check the main engineer workflows against the updated docs.

Documentation: update all documentation to match actual current status of the repo, with no planned features described as present.
