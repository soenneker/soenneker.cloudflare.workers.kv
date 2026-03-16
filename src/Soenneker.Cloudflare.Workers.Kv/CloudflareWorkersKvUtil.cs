using Microsoft.Kiota.Abstractions;
using Microsoft.Extensions.Logging;
using Soenneker.Cloudflare.OpenApiClient;
using Soenneker.Cloudflare.OpenApiClient.Accounts.Item.Storage.Kv.Namespaces.Item.Values.Item;
using Soenneker.Cloudflare.OpenApiClient.Models;
using Soenneker.Cloudflare.Utils.Client.Abstract;
using Soenneker.Cloudflare.Workers.Kv.Abstract;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Cloudflare.Workers.Kv;

/// <inheritdoc cref="ICloudflareWorkersKvUtil"/>
public sealed class CloudflareWorkersKvUtil : ICloudflareWorkersKvUtil
{
    private readonly ICloudflareClientUtil _clientUtil;
    private readonly ILogger<CloudflareWorkersKvUtil> _logger;

    public CloudflareWorkersKvUtil(ICloudflareClientUtil clientUtil, ILogger<CloudflareWorkersKvUtil> logger)
    {
        _clientUtil = clientUtil;
        _logger = logger;
    }

    public async ValueTask<Workers_kv_namespace_list_namespaces_200?> ListNamespaces(string accountId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        _logger.LogDebug("Listing KV namespaces for account {AccountId}", accountId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces.GetAsync(cancellationToken: cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list KV namespaces for account {AccountId}", accountId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_create_a_namespace_200?> CreateNamespace(string accountId, string title, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(title);
        _logger.LogInformation("Creating KV namespace {Title} for account {AccountId}", title, accountId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new Workers_kv_create_rename_namespace_body { Title = title };
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces.PostAsync(body, null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create KV namespace {Title} for account {AccountId}", title, accountId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_get_a_namespace_200?> GetNamespace(string accountId, string namespaceId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        _logger.LogDebug("Getting KV namespace {NamespaceId} for account {AccountId}", namespaceId, accountId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].GetAsync(null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get KV namespace {NamespaceId} for account {AccountId}", namespaceId, accountId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_rename_a_namespace_200?> RenameNamespace(string accountId, string namespaceId, string title, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentException.ThrowIfNullOrEmpty(title);
        _logger.LogInformation("Renaming KV namespace {NamespaceId} to {Title} for account {AccountId}", namespaceId, title, accountId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new Workers_kv_create_rename_namespace_body { Title = title };
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].PutAsync(body, null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to rename KV namespace {NamespaceId} for account {AccountId}", namespaceId, accountId);
            throw;
        }
    }

    public async ValueTask DeleteNamespace(string accountId, string namespaceId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        _logger.LogInformation("Deleting KV namespace {NamespaceId} for account {AccountId}", namespaceId, accountId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new Soenneker.Cloudflare.OpenApiClient.Accounts.Item.Storage.Kv.Namespaces.Item.WithNamespace_DeleteRequestBody();
        try
        {
            await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].DeleteAsync(body, null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete KV namespace {NamespaceId} for account {AccountId}", namespaceId, accountId);
            throw;
        }
    }

    public async ValueTask<Stream?> GetValue(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentException.ThrowIfNullOrEmpty(keyName);
        _logger.LogDebug("Getting KV value for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Values[keyName].GetAsync(null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get KV value for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
            throw;
        }
    }

    public async ValueTask<string?> GetValueAsString(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default)
    {
        await using Stream? stream = await GetValue(accountId, namespaceId, keyName, cancellationToken).NoSync();
        if (stream == null) return null;
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync(cancellationToken).NoSync();
    }

    public async ValueTask PutValue(string accountId, string namespaceId, string keyName, string value, int? expirationTtlSeconds = null, long? expirationUnixSeconds = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentException.ThrowIfNullOrEmpty(keyName);
        ArgumentNullException.ThrowIfNull(value);
        _logger.LogDebug("Putting KV value for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var multipartBody = new MultipartBody();
        multipartBody.AddOrReplacePart("value", "text/plain", value);
        Action<RequestConfiguration<WithKey_nameItemRequestBuilder.WithKey_nameItemRequestBuilderPutQueryParameters>>? requestConfig = null;
        if (expirationTtlSeconds.HasValue || expirationUnixSeconds.HasValue)
        {
            requestConfig = config =>
            {
                if (expirationTtlSeconds.HasValue) config.QueryParameters.ExpirationTtl = expirationTtlSeconds.Value.ToString();
                if (expirationUnixSeconds.HasValue) config.QueryParameters.Expiration = expirationUnixSeconds.Value.ToString();
            };
        }
        try
        {
            await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Values[keyName].PutAsync(multipartBody, requestConfig, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to put KV value for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
            throw;
        }
    }

    public async ValueTask DeleteKey(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentException.ThrowIfNullOrEmpty(keyName);
        _logger.LogDebug("Deleting KV key {KeyName} from namespace {NamespaceId}", keyName, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new WithKey_nameDeleteRequestBody();
        try
        {
            await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Values[keyName].DeleteAsync(body, null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete KV key {KeyName} from namespace {NamespaceId}", keyName, namespaceId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_list_a_namespace_s_keys_200?> ListKeys(string accountId, string namespaceId, string? prefix = null, int? limit = null, string? cursor = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        _logger.LogDebug("Listing KV keys in namespace {NamespaceId}", namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Keys.GetAsync(config =>
            {
                if (prefix != null) config.QueryParameters.Prefix = prefix;
                if (limit.HasValue) config.QueryParameters.Limit = limit.Value;
                if (cursor != null) config.QueryParameters.Cursor = cursor;
            }, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list KV keys in namespace {NamespaceId}", namespaceId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_read_the_metadata_for_a_key_200?> GetKeyMetadata(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentException.ThrowIfNullOrEmpty(keyName);
        _logger.LogDebug("Getting KV metadata for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Metadata[keyName].GetAsync(null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get KV metadata for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_get_multiple_key_value_pairs_200?> BulkGet(string accountId, string namespaceId, IReadOnlyList<string> keys, bool withMetadata = false, Workers_kv_namespace_get_multiple_key_value_pairs_type? type = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count > 100) throw new ArgumentException("Maximum 100 keys allowed for bulk get.", nameof(keys));
        _logger.LogDebug("Bulk getting {Count} KV keys from namespace {NamespaceId}", keys.Count, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new Workers_kv_namespace_get_multiple_key_value_pairs
        {
            Keys = new List<string>(keys),
            WithMetadata = withMetadata,
            Type = type
        };
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Bulk.GetPath.PostAsync(body, null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to bulk get KV keys from namespace {NamespaceId}", namespaceId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_write_multiple_key_value_pairs_200?> BulkPut(string accountId, string namespaceId, IReadOnlyList<Workers_kv_bulk_write_item> pairs, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentNullException.ThrowIfNull(pairs);
        if (pairs.Count > 10000) throw new ArgumentException("Maximum 10,000 pairs allowed for bulk put.", nameof(pairs));
        _logger.LogDebug("Bulk putting {Count} KV pairs to namespace {NamespaceId}", pairs.Count, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new List<Workers_kv_bulk_write_item>(pairs);
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Bulk.PutAsync(body, null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to bulk put KV pairs to namespace {NamespaceId}", namespaceId);
            throw;
        }
    }

    public async ValueTask<Workers_kv_namespace_delete_multiple_key_value_pairs_200?> BulkDelete(string accountId, string namespaceId, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count > 10000) throw new ArgumentException("Maximum 10,000 keys allowed for bulk delete.", nameof(keys));
        _logger.LogDebug("Bulk deleting {Count} KV keys from namespace {NamespaceId}", keys.Count, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new List<string>(keys);
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Bulk.DeletePath.PostAsync(body, null, cancellationToken).NoSync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to bulk delete KV keys from namespace {NamespaceId}", namespaceId);
            throw;
        }
    }
}
