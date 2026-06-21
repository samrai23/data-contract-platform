"""
scripts/test_mcp_server.py
===========================

Tests the MCP server using FastMCP's own async client.

WHY NOT CURL/REST FOR MCP?
---------------------------
MCP (Model Context Protocol) is NOT a REST API. It uses JSON-RPC over
Server-Sent Events (SSE). The server pushes responses via an SSE stream;
the client sends requests to a separate /messages/ endpoint.

REST (what curl does):
  client → POST /tools/list → server returns JSON immediately
           (simple request-response, no connection kept open)

SSE + JSON-RPC (what MCP does):
  client → GET /sse → server keeps connection open (SSE stream)
  client → POST /messages/?session_id=abc → server sends response via SSE stream
           (one persistent connection, many messages)

So there is NO /tools/list URL to hit with curl or Invoke-RestMethod.
FastMCP's Client handles the SSE handshake and JSON-RPC encoding for us.

USAGE:
    # Terminal 1 — keep this running
    cd services/mcp_server && python server.py

    # Terminal 2
    python scripts/test_mcp_server.py
"""

import asyncio
import json
import sys
import os

# contract-api must be running for the contract tools to work
CONTRACT_API = os.getenv("CONTRACT_API_URL", "http://localhost:8000")
MCP_SERVER   = os.getenv("MCP_SERVER_URL",  "http://localhost:8001")


def extract(call_result) -> object:
    """
    FastMCP 3.x client.call_tool() returns a CallToolResult object, not a
    plain Python value.  The actual data is in result.content[0].text as a
    JSON string.  This helper unpacks it to a plain Python object.

    Edge case: when a tool returns an empty list [], FastMCP encodes it as
    empty content blocks (content=[]).  In that case call_result.content is
    falsy, so we must return [] explicitly — NOT the CallToolResult object.
    """
    if not hasattr(call_result, "content"):
        return call_result          # not a CallToolResult — return as-is

    if not call_result.content:
        return []                   # tool returned empty result (e.g. empty list)

    text = call_result.content[0].text
    try:
        return json.loads(text)
    except (json.JSONDecodeError, AttributeError):
        return text                 # tool returned a plain string


async def main():
    try:
        from fastmcp import Client
    except ImportError:
        print("ERROR: fastmcp not installed. Run: pip install fastmcp")
        sys.exit(1)

    print(f"Connecting to MCP server at {MCP_SERVER}/sse ...")
    print("=" * 60)

    try:
        async with Client(f"{MCP_SERVER}/sse") as client:

            # ── 1. List all available tools ────────────────────────────
            print("\n[1] Listing available tools...")
            tools = await client.list_tools()
            for t in tools:
                print(f"    ✅  {t.name:25s} — {t.description.splitlines()[0][:60]}")

            # ── 2. list_vendors ────────────────────────────────────────
            print("\n[2] Calling list_vendors...")
            vendors = extract(await client.call_tool("list_vendors", {}))
            if vendors:
                for v in vendors:
                    print(f"    ✅  {v.get('vendor_id'):15s} v{v.get('version')}  {v.get('status')}")
            else:
                print("    (no vendors found — run seed_contracts.py first)")

            # ── 3. get_contract ────────────────────────────────────────
            print("\n[3] Calling get_contract for 'cars24'...")
            try:
                contract = extract(await client.call_tool("get_contract", {"vendor_id": "cars24"}))
                print(f"    vendor_id : {contract.get('vendor_id')}")
                print(f"    version   : {contract.get('version')}")
                print(f"    status    : {contract.get('status')}")
                fields = contract.get("fields", [])
                print(f"    fields    : {[f['name'] for f in fields]}")
            except Exception as e:
                print(f"    ⚠️  {e} (is contract-api running?)")

            # ── 4. get_drift_history ───────────────────────────────────
            print("\n[4] Calling get_drift_history for 'cars24'...")
            history = extract(await client.call_tool("get_drift_history", {"vendor_id": "cars24", "limit": 3}))
            if history:
                for h in history:
                    print(f"    {h}")
            else:
                print("    (empty — no drift events yet, expected at this stage)")

            print("\n" + "=" * 60)
            print("✅  MCP server is working correctly.")
            print("    Next: python scripts/test_agent_graph.py")

    except ConnectionRefusedError as e:
        print(f"\n❌  Could not connect to MCP server at {MCP_SERVER}")
        print(f"    Make sure it's running:")
        print(f"    cd services/mcp_server && python server.py")
        sys.exit(1)
    except Exception as e:
        import traceback
        print(f"\n❌  Unexpected error: {type(e).__name__}: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
