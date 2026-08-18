# MCP client configuration examples

These snippets assume `agent-memory-mcp` is installed in the environment the client launches. Replace endpoints and IDs with your own values; do not commit secrets.

## VS Code (stdio)

Create `.vscode/mcp.json` in your workspace:

```json
{
  "servers": {
    "agent-memory": {
      "type": "stdio",
      "command": "agent-memory-mcp",
      "args": [],
      "env": {
        "AGENT_MEMORY_MCP_TRANSPORT": "stdio",
        "AGENT_MEMORY_DEFAULT_USER_ID": "local-user",
        "COSMOS_DB_ENDPOINT": "https://<account>.documents.azure.com:443/",
        "COSMOS_DB_DATABASE": "ai_memory",
        "AI_FOUNDRY_ENDPOINT": "https://<resource>.openai.azure.com/",
        "AI_FOUNDRY_EMBEDDING_DEPLOYMENT_NAME": "text-embedding-3-large",
        "AI_FOUNDRY_CHAT_DEPLOYMENT_NAME": "gpt-4o-mini"
      }
    }
  }
}
```

You can use the same `servers` object under the VS Code `mcp` setting if your setup manages MCP servers from user/workspace settings.

## Claude Desktop (stdio)

Add an entry to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "agent-memory": {
      "command": "agent-memory-mcp",
      "args": [],
      "env": {
        "AGENT_MEMORY_MCP_TRANSPORT": "stdio",
        "AGENT_MEMORY_DEFAULT_USER_ID": "local-user",
        "COSMOS_DB_ENDPOINT": "https://<account>.documents.azure.com:443/",
        "COSMOS_DB_DATABASE": "ai_memory",
        "AI_FOUNDRY_ENDPOINT": "https://<resource>.openai.azure.com/",
        "AI_FOUNDRY_EMBEDDING_DEPLOYMENT_NAME": "text-embedding-3-large",
        "AI_FOUNDRY_CHAT_DEPLOYMENT_NAME": "gpt-4o-mini"
      }
    }
  }
}
```

## Remote Streamable HTTP client

For a deployed server, configure a Streamable HTTP transport pointing at the MCP path (default `/mcp`) and send an Entra access token for the configured audience/scopes.

```json
{
  "servers": {
    "agent-memory": {
      "type": "streamable-http",
      "url": "https://<app-name>.<region>.azurecontainerapps.io/mcp",
      "headers": {
        "Authorization": "Bearer <access-token>"
      }
    }
  }
}
```

The token must match `ENTRA_TENANT_ID`, `ENTRA_AUDIENCE`, and any `AGENT_MEMORY_REQUIRED_SCOPES` configured on the server. In hosted mode, tools bind `user_id` to the token subject unless the trusted `AGENT_MEMORY_ALLOW_USER_ID_ARG` override is enabled.
