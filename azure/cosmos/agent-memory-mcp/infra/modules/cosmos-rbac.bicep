// Cosmos DB data-plane RBAC for the Container App managed identity.
// This module must be scoped to the resource group that contains the Cosmos account.

@description('Name of the existing Cosmos account in this module\'s scope.')
param cosmosAccountName string

@description('Principal ID of the managed identity used by the Container App.')
param principalId string

resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' existing = {
  name: cosmosAccountName
}

var cosmosDataContributorRoleId = '00000000-0000-0000-0000-000000000002'

resource cosmosContributorRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = {
  parent: cosmos
  name: guid(cosmos.id, principalId, cosmosDataContributorRoleId)
  properties: {
    roleDefinitionId: '${cosmos.id}/sqlRoleDefinitions/${cosmosDataContributorRoleId}'
    principalId: principalId
    scope: cosmos.id
  }
}
