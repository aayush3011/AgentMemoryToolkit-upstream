@description('Name of the Container Apps managed environment.')
param managedEnvironmentName string

@description('Name of the Log Analytics workspace used by the managed environment.')
param logAnalyticsWorkspaceName string

@description('Name of the Container App.')
param containerAppName string

@description('Azure region.')
param location string

@description('Container image reference to deploy.')
param image string

@description('User-assigned managed identity resource ID.')
param uamiResourceId string

@description('User-assigned managed identity client ID.')
param uamiClientId string

@description('Environment variables for the MCP container.')
param env array

@description('Container port for Streamable HTTP.')
param targetPort int = 8080

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

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsWorkspaceName
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

resource managedEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: managedEnvironmentName
  location: location
  tags: tags
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: workspace.properties.customerId
        sharedKey: workspace.listKeys().primarySharedKey
      }
    }
  }
}

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: containerAppName
  location: location
  tags: union(tags, {
    'azd-service-name': 'agent-memory-mcp'
  })
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${uamiResourceId}': {}
    }
  }
  properties: {
    managedEnvironmentId: managedEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: targetPort
        transport: 'auto'
        allowInsecure: false
      }
    }
    template: {
      containers: [
        {
          name: 'agent-memory-mcp'
          image: image
          env: concat(env, [
            {
              name: 'AZURE_CLIENT_ID'
              value: uamiClientId
            }
          ])
          resources: {
            cpu: json(cpu)
            memory: memory
          }
        }
      ]
      scale: {
        minReplicas: minReplicas
        maxReplicas: maxReplicas
      }
    }
  }
}

output name string = containerApp.name
output fqdn string = containerApp.properties.configuration.ingress.fqdn
