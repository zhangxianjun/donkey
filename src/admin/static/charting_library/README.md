This directory is served at `/charting_library/`.

Files under this directory are loaded from the same origin as the admin UI to avoid
browser CORS failures when the TradingView widget requests its chunk files.

Current behavior:
- `charting_library.js` is the local TradingView entry bundle mirrored into the repo.
- The admin backend serves any existing local asset directly from this directory.
- If a requested asset is missing, the backend fetches it from TradingView's hosted
  `charting_library/` base URL, stores it here, and then serves it from the local
  same-origin path on subsequent requests.
