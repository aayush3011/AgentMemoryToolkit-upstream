// Agent Memory MCP — Container Apps deployment over existing Agent Memory Toolkit resources.

targetScope = 'resourceGroup'

@minLength(1)
@maxLength(64)
@description('Environment/name prefix used to derive resource names.')
param environmentName string

@description('Azure region for Container Apps resources.')
param location string = resourceGroup().location

@description('Container image reference to deploy, for example myacr.azurecr.io/agent-memory-mcp:latest.')
param containerImage string

@description('Name of the existing Cosmos DB account.')
param cosmosAccountName string

@description('Resource group containing the existing Cosmos DB account. Defaults to this deployment resource group.')
param cosmosAccountResourceGroupName string = resourceGroup().name

@description('Cosmos database name.')
param cosmosDatabaseName string = 'ai_memory'

@description('Memories container name.')
param memoriesContainerName string = 'memories'

@description('Turns container name.')
param turnsContainerName string = 'memories_turns'

@description('Summaries container name.')
param summariesContainerName string = 'memories_summaries'

@description('Counter container name used by the processor trigger counters.')
param countersContainerName string = 'counter'

@description('Lease container name used by the durable/function processor topology.')
param leaseContainerName string = 'leases'

@description('AI Foundry / Azure OpenAI endpoint. Leave empty if processing/search embeddings are not used by this server.')
param aiFoundryEndpoint string = ''

@description('Embedding deployment name.')
param embeddingDeploymentName string = 'text-embedding-3-large'

@description('Optional embedding dimensions override. Empty means SDK default.')
param embeddingDimensions string = ''

@description('Chat deployment name.')
param chatDeploymentName string = 'gpt-4o-mini'

@description('Enable turn embeddings for raw conversation search.')
param enableTurnEmbeddings bool = false

@description('Backend that owns processing. `durable` is recommended for hosted thin-writer mode.')
@allowed([
  'durable'
  'inprocess'
])
param memoryProcessorOwner string = 'durable'

@description('Streamable HTTP bind port.')
param mcpPort int = 8080

@description('Streamable HTTP endpoint path.')
param mcpHttpPath string = '/mcp'

@description('Run FastMCP Streamable HTTP in stateless mode.')
param statelessHttp bool = true

@description('MCP server log level.')
param logLevel string = 'INFO'

@description('Default user id for unauthenticated/local-style deployments. Leave empty for hosted auth.')
param defaultUserId string = ''

@description('Whether tools may accept a client-supplied user_id that differs from the token identity.')
param allowUserIdArg bool = false

@description('Expose advanced granular processing tools.')
param exposeGranular bool = false

@description('Maximum top_k accepted by search tools.')
param maxTopK int = 50

@description('Create database/containers on startup. Keep false when infra owns the topology.')
param autoCreate bool = false

@description('Enable Entra bearer-token authentication for hosted Streamable HTTP.')
param authEnabled bool = true

@description('Entra tenant id used to validate incoming bearer tokens.')
param entraTenantId string = subscription().tenantId

@description('Expected token audience/client id for this MCP resource server.')
param entraAudience string

@description('Public resource-server URL advertised by MCP auth metadata. Empty lets the server default to the issuer URL.')
param resourceServerUrl string = ''

@description('JWT claim used as the memory owner id.')
param userIdClaim string = 'oid'

@description('Space- or comma-separated scopes/roles required on the access token. Empty disables scope enforcement.')
param requiredScopes string = ''

@description('Minimum Container App replicas.')
param minReplicas int = 1

@description('Maximum Container App replicas.')
param maxReplicas int = 3

@description('CPU cores allocated to the container.')
param cpu string = '0.5'

@description('Memory allocated to the container.')
param memory string = '1Gi'

@description('Tags to apply.')
param tags object = {}

var resourceToken = take(uniqueString(resourceGroup().id, environmentName), 13)
var namePrefix = take(replace(toLower(environmentName), '_', '-'), 12)
var uamiName = 'id-${namePrefix}-${resourceToken}'
var managedEnvironmentName = 'cae-${namePrefix}-${resourceToken}'
var logAnalyticsWorkspaceName = 'log-${namePrefix}-${resourceToken}'
var containerAppName = 'ca-${namePrefix}-${resourceToken}'
var commonTags = union(tags, {
  'azd-env-name': environmentName
  workload: 'azure-cosmos-agent-memory-mcp'
})

resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' existing = {
  name: cosmosAccountName
  scope: resourceGroup(cosmosAccountResourceGroupName)
}

module identity 'modules/identity.bicep' = {
  name: 'identity'
  params: {
    name: uamiName
    location: location
    tags: commonTags
  }
}

module cosmosRbac 'modules/cosmos-rbac.bicep' = {
  scope: resourceGroup(cosmosAccountResourceGroupName)
  name: 'cosmos-rbac-agent-memory-mcp'
  params: {
    cosmosAccountName: cosmosAccountName
    principalId: identity.outputs.principalId
  }
}

var baseEnv = [
  {
    name: 'COSMOS_DB_ENDPOINT'
    value: cosmos.properties.documentEndpoint
  }
  {
    name: 'COSMOS_DB_DATABASE'
    value: cosmosDatabaseName
  }
  {
    name: 'COSMOS_DB_MEMORIES_CONTAINER'
    value: memoriesContainerName
  }
  {
    name: 'COSMOS_DB_SUMMARIES_CONTAINER'
    value: summariesContainerName
  }
  {
    name: 'COSMOS_DB_TURNS_CONTAINER'
    value: turnsContainerName
  }
  {
    name: 'COSMOS_DB_COUNTERS_CONTAINER'
    value: countersContainerName
  }
  {
    name: 'COSMOS_DB_LEASE_CONTAINER'
    value: leaseContainerName
  }
  {
    name: 'AI_FOUNDRY_ENDPOINT'
    value: aiFoundryEndpoint
  }
  {
    name: 'AI_FOUNDRY_EMBEDDING_DEPLOYMENT_NAME'
    value: embeddingDeploymentName
  }
  {
    name: 'AI_FOUNDRY_CHAT_DEPLOYMENT_NAME'
    value: chatDeploymentName
  }
  {
    name: 'ENABLE_TURN_EMBEDDINGS'
    value: string(enableTurnEmbeddings)
  }
  {
    name: 'MEMORY_PROCESSOR_OWNER'
    value: memoryProcessorOwner
  }
  {
    name: 'AGENT_MEMORY_MCP_TRANSPORT'
    value: 'streamable-http'
  }
  {
    name: 'AGENT_MEMORY_MCP_HOST'
    value: '0.0.0.0'
  }
  {
    name: 'AGENT_MEMORY_MCP_PORT'
    value: string(mcpPort)
  }
  {
    name: 'AGENT_MEMORY_MCP_HTTP_PATH'
    value: mcpHttpPath
  }
  {
    name: 'AGENT_MEMORY_MCP_STATELESS_HTTP'
    value: string(statelessHttp)
  }
  {
    name: 'AGENT_MEMORY_MCP_LOG_LEVEL'
    value: logLevel
  }
  {
    name: 'AGENT_MEMORY_DEFAULT_USER_ID'
    value: defaultUserId
  }
  {
    name: 'AGENT_MEMORY_ALLOW_USER_ID_ARG'
    value: string(allowUserIdArg)
  }
  {
    name: 'AGENT_MEMORY_EXPOSE_GRANULAR'
    value: string(exposeGranular)
  }
  {
    name: 'AGENT_MEMORY_MAX_TOP_K'
    value: string(maxTopK)
  }
  {
    name: 'AGENT_MEMORY_AUTO_CREATE'
    value: string(autoCreate)
  }
  {
    name: 'AGENT_MEMORY_AUTH_ENABLED'
    value: string(authEnabled)
  }
  {
    name: 'ENTRA_TENANT_ID'
    value: entraTenantId
  }
  {
    name: 'ENTRA_AUDIENCE'
    value: entraAudience
  }
  {
    name: 'AGENT_MEMORY_RESOURCE_SERVER_URL'
    value: resourceServerUrl
  }
  {
    name: 'AGENT_MEMORY_USER_ID_CLAIM'
    value: userIdClaim
  }
  {
    name: 'AGENT_MEMORY_REQUIRED_SCOPES'
    value: requiredScopes
  }
]

var mcpEnv = empty(embeddingDimensions) ? baseEnv : concat(baseEnv, [
  {
    name: 'AI_FOUNDRY_EMBEDDING_DIMENSIONS'
    value: embeddingDimensions
  }
])

module containerApp 'modules/container-app.bicep' = {
  name: 'container-app'
  params: {
    managedEnvironmentName: managedEnvironmentName
    logAnalyticsWorkspaceName: logAnalyticsWorkspaceName
    containerAppName: containerAppName
    location: location
    image: containerImage
    uamiResourceId: identity.outputs.id
    uamiClientId: identity.outputs.clientId
    env: mcpEnv
    targetPort: mcpPort
    minReplicas: minReplicas
    maxReplicas: maxReplicas
    cpu: cpu
    memory: memory
    tags: commonTags
  }
  dependsOn: [
    cosmosRbac
  ]
}

output AZURE_CLIENT_ID string = identity.outputs.clientId
output CONTAINER_APP_NAME string = containerApp.outputs.name
output CONTAINER_APP_FQDN string = containerApp.outputs.fqdn
output AGENT_MEMORY_MCP_URL string = 'https://${containerApp.outputs.fqdn}${mcpHttpPath}'
output COSMOS_DB_ENDPOINT string = cosmos.properties.documentEndpoint
