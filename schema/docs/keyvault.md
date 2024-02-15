## Using values from Azure Key Vault

The DB extractor also supports loading values from Azure Key Vault. To load a configuration value from Azure Key Vault, use the `!keyvault` tag followed by the name of the secret you want to load. For example, to load the value of the `my-secret-name` secret in Key Vault into a `password` parameter, configure your extractor like this:

``` yaml
password: !keyvault my-secret-name
```

To use Key Vault, you also need to include the `azure-keyvault` section in your configuration, with the following parameters:

| Parameter | Description |
| - | - |
| `keyvault-name` | Name of Key Vault to load secrets from |
| `authentication-method` | How to authenticate to Azure. Either `default` or `client-secret`. For `default`, the extractor will look at the user running the extractor, and look for pre-configured Azure logins from tools like the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli). For `client-secret`, the extractor will authenticate with a configured client ID/secret pair.
| `client-id` | Required for using the `client-secret` authentication method. The client ID to use when authenticating to Azure. |
| `secret` | Required for using the `client-secret` authentication method. The client secret to use when authenticating to Azure. |
| `tenant-id` | Required for using the `client-secret` authentication method. The tenant ID of the Key Vault in Azure. |

__Example:__

``` yaml
azure-keyvault:
  keyvault-name: my-keyvault-name
  authentication-method: client-secret
  tenant-id: 6f3f324e-5bfc-4f12-9abe-22ac56e2e648
  client-id: 6b4cc73e-ee58-4b61-ba43-83c4ba639be6
  secret: 1234abcd
```
