# ConversationAnalyser (cleaned)

## What changed
- Reduced the project to the minimum working files.
- Removed the unused `Genesys.Core` module copy, test scripts, docs, sample data, profiles, editor settings, and Git metadata.
- Fixed the main stability blockers:
  - streaming JSONL load instead of `ReadAllLines()`
  - streaming CSV/JSONL export instead of buffering full export sets in memory
  - O(1) conversation lookup by `conversationId` for result selection
  - capped WPF grid rendering to keep the UI responsive on large result sets
  - safer export path with redaction enabled by default
  - reliable writer disposal for export paths

## Files
- `GenesysConvAnalyzer.ps1` - main WPF app
- `src/ui/UiApiRetry.ps1` - retry/backoff helper used by the app

## Notes
- The grid now shows at most the first 20,000 rows to avoid UI lockups.
- Exports still include all loaded conversations.
- The **Redact exports** checkbox is enabled by default. Uncheck it only if raw export is explicitly needed in a trusted environment.

## Run
```powershell
.\GenesysConvAnalyzer.ps1
```

Or specify a config file:
```powershell
.\GenesysConvAnalyzer.ps1 -ConfigPath .\GenesysCore-GUI.config.json
```
