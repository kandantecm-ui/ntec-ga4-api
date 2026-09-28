# NTEC Analytics API and MCP

This repository serves the existing GPT Actions API and a read-only MCP endpoint from the same FastAPI process. The original `/api/...`, `/health`, and `/openapi.json` routes retain their paths and behavior. MCP tools call those routes internally, so there is one implementation of each report.

## Render deployment check

There is no `render.yaml` in this repository. The Render dashboard settings and current environment variable values cannot be verified from GitHub. Before merging, check that the existing web service uses this repository and deploys `main`, that the build installs `requirements.txt`, and that the start command imports `ga4_proxy_oauth:app` (for example, `uvicorn ga4_proxy_oauth:app --host 0.0.0.0 --port $PORT`). Keep the current Google credentials and property/dataset settings in Render; this change does not alter them.

Set `MCP_ALLOWED_HOSTS` to the public hostname **without** `https://` or a path, such as `ntec-ga4-api.onrender.com`. The MCP SDK permits localhost by default here, and the code also reads `RENDER_EXTERNAL_HOSTNAME` if Render supplies it. Explicitly setting `MCP_ALLOWED_HOSTS` avoids a `421 Invalid Host header` when Render uses a custom domain. Restart the service after changing settings. The connector URL is `https://<Render-host>/mcp/` (include the trailing slash). Check `/health` and scan MCP tools after deployment.

The new Python dependency is pinned as `mcp==2.2.0`. Render's build must install `requirements.txt`, not the smaller `requirements_ga4_proxy.txt`, which omits the analytics dependencies required by this application's existing routes.

## Authentication and access

| Surface | Current request authentication | Effect of this PR |
| --- | --- | --- |
| Existing GPT Actions `/api/...` | None, as configured in the GPT | No change |
| New `/mcp/` endpoint | None | 28 read-only report tools; same service-account-backed data as `/api/...` |
| Google APIs | Render service account and Ads credentials | No change |

The MCP connection can be configured with **no authentication**, matching the existing GPT Action. This is a public endpoint: anyone who knows the URL can request analytics and anonymous BigQuery user paths. A custom app's visibility setting does not protect the web endpoint. For private access, plan an OAuth-protected MCP connection and review protection of the existing `/api/...` Action separately. Do not put Google credentials or a static secret in this repository or in an MCP URL. No OAuth or bearer-token flow is implied by the Google service account; it authenticates the server **to Google**, not callers **to this server**.

The general analysis tool is `getAcquisitionOpportunitySummary`. With no dates it uses the 30 complete days ending yesterday in Japan time. Other tools take `params` with the same JSON fields as the corresponding `/api/...` request model; their path is in each tool's description, and `/openapi.json` describes the fields. MCP is read-only and returns the existing route's JSON, including validation errors with their HTTP status.

## Local check

```bash
pip install -r requirements.txt
python -m unittest discover -s tests
uvicorn ga4_proxy_oauth:app --host 127.0.0.1 --port 8000
```

The unit test does not call Google. Live data and Render behavior must be checked after deployment with the actual credentials and hostname.
