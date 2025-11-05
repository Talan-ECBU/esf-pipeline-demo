param location string = resourceGroup().location
param sqlLocation string = 'uksouth'
param storageAccountName string = 'esf_online_safety'
param sqlServerName string = 'esf-sqlserver-001'
param sqlAdminUsername string = 'sqladmin'
@secure()
param sqlAdminPassword string

// Create Storage Account
resource storageAccount 'Microsoft.Storage/storageAccounts@2024-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
  }
}

// Create Blob Containers
var containers = [
  'raw-scraped'
  'processed'
  'models-training'
  'archive'
]

resource blobContainers 'Microsoft.Storage/storageAccounts/blobServices/containers@2024-01-01' = [for name in containers: {
  name: '${storageAccount.name}/default/${name}'
  properties: {
    publicAccess: 'None'
  }
}]

// Lifecycle Rules
resource lifecycleMgmt 'Microsoft.Storage/storageAccounts/managementPolicies@2024-01-01' = {
  name: 'default'
  parent: storageAccount
  properties: {
    policy: {
      rules: [
        {
          enabled: true
          name: 'lifecycleCoolArchive'
          type: 'Lifecycle'
          definition: {
            actions: {
              baseBlob: {
                tierToCool: {
                  daysAfterModificationGreaterThan: 30
                }
                tierToArchive: {
                  daysAfterModificationGreaterThan: 180
                }
              }
            }
            filters: {
              blobTypes: ['blockBlob']
            }
          }
        }
      ]
    }
  }
}

// Azure SQL Server
resource sqlServer 'Microsoft.Sql/servers@2024-05-01-preview' = {
  name: sqlServerName
  location: sqlLocation
  properties: {
    administratorLogin: sqlAdminUsername
    administratorLoginPassword: sqlAdminPassword
    version: '12.0'
  }
}

// Azure SQL DB
resource sqlDb 'Microsoft.Sql/servers/databases@2024-05-01-preview' = {
  parent: sqlServer
  name: 'esfmetadata'
  location: sqlLocation
  sku: {
    name: 'Basic'
    tier: 'Basic'
    capacity: 5
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    maxSizeBytes: 2147483648
  }
}
