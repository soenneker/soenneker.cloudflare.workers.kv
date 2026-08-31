[![](https://img.shields.io/nuget/v/soenneker.cloudflare.workers.kv.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.cloudflare.workers.kv/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.cloudflare.workers.kv/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.cloudflare.workers.kv/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.cloudflare.workers.kv.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.cloudflare.workers.kv/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.cloudflare.workers.kv/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.cloudflare.workers.kv/actions/workflows/codeql.yml)

# Soenneker.Cloudflare.Workers.Kv

Manages Cloudflare Workers KV namespaces, values, metadata, key listings, and bulk operations.

## Installation

```bash
dotnet add package Soenneker.Cloudflare.Workers.Kv
```

## Registration

```csharp
using Soenneker.Cloudflare.Workers.Kv.Registrars;

services.AddCloudflareWorkersKvUtilAsScoped();
```

Singleton registration is also available. Every operation accepts an API token explicitly, allowing one utility instance to work with different authorized Cloudflare accounts. Use a bounded set of tokens because the underlying generated and HTTP clients are cached per token.

## Values

```csharp
using Soenneker.Cloudflare.Workers.Kv.Abstract;

await kv.PutValue(
    accountId,
    apiToken,
    namespaceId,
    keyName: "users/42/profile",
    value: json,
    expirationTtlSeconds: 3600,
    cancellationToken: cancellationToken);

string? value = await kv.GetValueAsString(
    accountId,
    apiToken,
    namespaceId,
    "users/42/profile",
    cancellationToken);
```

`GetValue` returns the response stream without buffering it; the caller owns and must dispose that stream. `GetValueAsString` reads and disposes it. A missing value or metadata response returns `null`; other generated API failures propagate with status details.

`PutValue` accepts either a relative TTL in seconds, an absolute Unix expiration timestamp, both, or neither. Confirm the intended Cloudflare expiration behavior before supplying both values.

## Listing and bulk operations

`ListKeys` exposes Cloudflare's prefix, limit, and cursor parameters. It returns one page; continue with the response cursor until Cloudflare reports no further page.

- `BulkGet` accepts at most 100 keys.
- `BulkPut` accepts at most 10,000 generated `WorkersKvBulkWriteItem` values.
- `BulkDelete` accepts at most 10,000 keys and is destructive.

The utility sends each bulk request as one Cloudflare API operation; it does not split oversized collections, retry partial results, or provide transactional rollback.

## Namespaces

`CreateNamespace`, `GetNamespace`, `RenameNamespace`, and `ListNamespaces` expose the generated response envelopes. `DeleteNamespace` permanently deletes the namespace and its stored data; it does not ask for confirmation.
