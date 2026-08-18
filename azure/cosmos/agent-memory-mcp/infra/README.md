# Agent Memory MCP Container Apps deployment

This Bicep deploys the Python `agent-memory-mcp` server to Azure Container Apps and grants its user-assigned managed identity the Cosmos DB Built-in Data Contributor data-plane role. No Cosmos keys are used.

## Build and push

From `azure/cosmos/agent-memory-mcp`:

```bash
az acr build --registry <acr-name> --image agent-memory-mcp:latest .
# or: docker build -t <acr-login-server>/agent-memory-mcp:latest . && docker push <acr-login-server>/agent-memory-mcp:latest
```

## Deploy

Required values: `environmentName`, `containerImage`, `cosmosAccountName`, and `entraAudience`; set `cosmosAccountResourceGroupName` if the Cosmos account is in a different resource group. Optional: AI Foundry endpoint, deployment names, scopes, and container names. The included `main.parameters.json` follows the repo's `${VAR}` placeholder convention; replace those placeholders via your deployment tooling or pass explicit CLI overrides.

```bash
az deployment group create \
  --resource-group <container-app-rg> \
  --template-file infra/main.bicep \
  --parameters @infra/main.parameters.json \
  --parameters \
      environmentName=<env-name> \
      containerImage=<acr-login-server>/agent-memory-mcp:latest \
      cosmosAccountName=<cosmos-account> \
      entraAudience=<app-client-id-or-api-uri>
```

The deployment outputs `AGENT_MEMORY_MCP_URL`, normally `https://<app-fqdn>/mcp`.

## MCP client

Configure a Streamable HTTP MCP client to use the output URL and send an Entra access token whose audience matches `ENTRA_AUDIENCE` and scopes/roles satisfy `AGENT_MEMORY_REQUIRED_SCOPES` when configured.

Cosmos access uses the Container App managed identity (`AZURE_CLIENT_ID`) plus Cosmos DB SQL RBAC; do not configure `COSMOS_DB_KEY` in hosted deployments.
