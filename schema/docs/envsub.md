## Using values from environment variables

The configuration file allows substitutions with environment variables. For example:

```yaml
cognite:
  secret: ${COGNITE_CLIENT_SECRET}
```

will load the value from the `COGNITE_CLIENT_SECRET` environment variable into the `cognite/secret` parameter. You can also do string interpolation with environment variables, for example:

```yaml
url: http://my-host.com/api/endpoint?secret=${MY_SECRET_TOKEN}
```

:::info Note
Implicit substitutions only work for unquoted value strings. For quoted strings, use the `!env` tag to activate environment substitution:

```yaml
url: !env 'http://my-host.com/api/endpoint?secret=${MY_SECRET_TOKEN}'
```
:::
