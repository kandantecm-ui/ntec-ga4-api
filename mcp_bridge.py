"""Read-only MCP transport for the existing NTEC analytics HTTP API."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import os
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations


# Keep this list explicit: MCP must expose only the existing read-only reports.
# Every tool invokes the corresponding /api route, including its validation and errors.
REPORTS = {
    "getAcquisitionOpportunitySummary": "/api/acquisition/opportunity-summary",
    "getDashboardSummary": "/api/dashboard/summary",
    "getChannelReport": "/api/ga4/standard/channel",
    "getCampaignReport": "/api/ga4/acquisition/campaigns",
    "getConversionSummary": "/api/ga4/conversion/summary",
    "getConversionPagesReport": "/api/ga4/conversion/pages",
    "getConversionPathReport": "/api/ga4/conversion/path",
    "getLandingPageConversionReport": "/api/ga4/conversion/landing-pages",
    "getThanksPageSummary": "/api/ga4/conversion/thanks-summary",
    "getPagePerformanceReport": "/api/ga4/page/performance",
    "getExitPagesReport": "/api/ga4/page/exits",
    "getColumnRankingReport": "/api/ga4/column/ranking",
    "getPageFlow": "/api/ga4/page/flow",
    "getPageFlowFromPage": "/api/ga4/page/flow/from-page",
    "getPreviousPage": "/api/ga4/page/before-page",
    "getUsersByPage": "/api/bq/page/users",
    "getUserPathsByTarget": "/api/bq/user/path",
    "getUserJourney": "/api/bq/user/journey",
    "getPrePagesBeforeTarget": "/api/bq/page/pre-pages",
    "getConversionPrePages": "/api/bq/conversion/pre-pages",
    "getContentConversionContribution": "/api/bq/content/conversion-contribution",
    "getSearchConsoleSites": "/api/search-console/sites",
    "getSearchConsoleKeywords": "/api/search-console/keywords",
    "getSearchConsolePagesReport": "/api/search-console/pages",
    "getSeoOpportunityReport": "/api/search-console/seo-opportunities",
    "getSearchConsoleQueryReport": "/api/search-console/query",
    "getKeywordSearchVolume": "/api/google-ads/keyword/search-volume",
    "getKeywordOpportunityReport": "/api/keyword/opportunities",
}


def install_mcp(app: FastAPI) -> None:
    """Mount /mcp without changing any existing /api route or its authentication."""
    server = MCPServer(
        "NTEC Analytics",
        instructions=(
            "NTEC site analytics, read-only. Start with "
            "getAcquisitionOpportunitySummary for a general analysis. "
            "Pass the same JSON fields as the corresponding /api endpoint in params. "
            "Dates are YYYY-MM-DD in Japan time. Do not treat anonymous GA4 IDs "
            "as real-world identities."
        ),
    )

    api_routes = {route.path: route for route in app.routes if route.path.startswith("/api/")}
    missing = set(REPORTS.values()) - set(api_routes)
    if missing:
        raise RuntimeError(f"MCP report routes missing: {sorted(missing)}")

    for name, path in REPORTS.items():
        route = api_routes[path]
        method = "GET" if "GET" in route.methods else "POST"
        description = (
            f"Read the NTEC analytics report at {path}. "
            "params contains its existing API JSON request fields; "
            f"see /openapi.json for the {route.name} request schema."
        )

        def register(report_name: str, report_path: str, report_method: str, doc: str) -> None:
            @server.tool(
                name=report_name,
                description=doc,
                annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False),
            )
            async def report(params: dict[str, Any] | None = None) -> dict[str, Any]:
                payload = dict(params or {})
                if report_name == "getAcquisitionOpportunitySummary":
                    # This HTTP endpoint requires both dates. Use a completed 30-day
                    # window in JST when the MCP caller omits them.
                    yesterday = datetime.now(ZoneInfo("Asia/Tokyo")).date() - timedelta(days=1)
                    payload.setdefault("endDate", yesterday.isoformat())
                    payload.setdefault("startDate", (yesterday - timedelta(days=29)).isoformat())

                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://localhost", timeout=120, trust_env=False
                ) as client:
                    if report_method == "GET":
                        response = await client.get(report_path, params=payload)
                    else:
                        response = await client.post(report_path, json=payload)
                result = response.json()
                if response.is_error:
                    return {"error": result, "status_code": response.status_code}
                return result

        register(name, path, method, description)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        async with server.session_manager.run():
            yield

    # Mounting a sub-app does not start its lifespan; the parent owns the manager.
    app.router.lifespan_context = lifespan
    hosts = ["localhost", "localhost:*", "127.0.0.1", "127.0.0.1:*"]
    for host in os.getenv("MCP_ALLOWED_HOSTS", "").split(","):
        host = host.strip()
        if host:
            hosts.extend([host, f"{host}:*"])
    render_host = os.getenv("RENDER_EXTERNAL_HOSTNAME", "").strip()
    if render_host:
        hosts.extend([render_host, f"{render_host}:*"])
    app.mount(
        "/mcp",
        server.streamable_http_app(
            streamable_http_path="/",
            stateless_http=True,
            json_response=True,
            transport_security=TransportSecuritySettings(allowed_hosts=hosts),
        ),
    )
