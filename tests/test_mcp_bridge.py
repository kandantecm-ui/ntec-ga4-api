import unittest

from fastapi.testclient import TestClient

from ga4_proxy_oauth import app
from mcp_bridge import REPORTS


class MCPBridgeTest(unittest.TestCase):
    def test_existing_routes_and_mcp_transport(self):
        headers = {
            "host": "localhost",
            "accept": "application/json, text/event-stream",
            "mcp-protocol-version": "2025-11-25",
        }
        with TestClient(app) as client:
            self.assertEqual(client.get("/health").status_code, 200)
            self.assertIn("/api/acquisition/opportunity-summary", client.get("/openapi.json").json()["paths"])
            response = client.post(
                "/mcp/",
                headers=headers,
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
            )
            self.assertEqual(response.status_code, 200)
            names = {tool["name"] for tool in response.json()["result"]["tools"]}
            self.assertEqual(names, set(REPORTS))

            # The MCP tool runs the real API validation; no Google call is made.
            response = client.post(
                "/mcp/",
                headers=headers,
                json={
                    "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": {"name": "getUsersByPage", "arguments": {"params": {}}},
                },
            )
            self.assertEqual(response.status_code, 200)
            result = response.json()["result"]["structuredContent"]
            self.assertEqual(result["status_code"], 422)
            self.assertEqual(result["error"]["detail"][0]["loc"], ["body", "targetPage"])


if __name__ == "__main__":
    unittest.main()
