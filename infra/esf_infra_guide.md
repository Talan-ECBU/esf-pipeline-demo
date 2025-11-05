# ESF Azure Infrastructure Deployment Guide

## Prerequisites

Make sure you have the following tools installed before proceeding:

- **Azure CLI** (version 2.20.0 or later): [Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- **Bicep CLI** (installed automatically with Azure CLI >= 2.26): [Learn more](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/install)

Login to your Azure account:

```bash
az login
```

Set your target subscription (if needed):

```bash
az account set --subscription "<your-subscription-name-or-id>"
```

## Deploying Infrastructure with Bicep

Use the following command to deploy the infrastructure defined in your `esf-infra.bicep` file using the parameter values in `parameters.json`:

```bash
az deployment group create \
  --resource-group ESF_ONLINE_SAFETY \
  --template-file esf-infra.bicep \
  --parameters @parameters.json
```

## Deleting the Resource Group

To delete the **entire resource group** and everything inside it:

```bash
az group delete --name rg-test --yes --no-wait
```

> ⚠️ This permanently deletes all resources in the resource group.
