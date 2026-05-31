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
using System.Linq;
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

    public async ValueTask<WorkersKvNamespaceListNamespaces200?> ListNamespaces(string accountId, CancellationToken cancellationToken = default)
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

    public async ValueTask<WorkersKvNamespaceCreateANamespace200?> CreateNamespace(string accountId, string title, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(title);
        _logger.LogInformation("Creating KV namespace {Title} for account {AccountId}", title, accountId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new WorkersKvCreateRenameNamespaceBody { Title = title };
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

    public async ValueTask<WorkersKvNamespaceGetANamespace200?> GetNamespace(string accountId, string namespaceId, CancellationToken cancellationToken = default)
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

    public async ValueTask<WorkersKvNamespaceRenameANamespace200?> RenameNamespace(string accountId, string namespaceId, string title, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentException.ThrowIfNullOrEmpty(title);
        _logger.LogInformation("Renaming KV namespace {NamespaceId} to {Title} for account {AccountId}", namespaceId, title, accountId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new WorkersKvCreateRenameNamespaceBody { Title = title };
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
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Values[EncodeKeyName(keyName)].GetAsync(null, cancellationToken).NoSync();
        }
        catch (WorkersKvApiResponseCommonFailure ex) when (ex.ResponseStatusCode == 404)
        {
            _logger.LogDebug("KV value for key {KeyName} in namespace {NamespaceId} was not found", keyName, namespaceId);
            return null;
        }
        catch (WorkersKvApiResponseCommonFailure ex)
        {
            _logger.LogError(ex, "Failed to get KV value for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
            throw CreateFailureException("get", keyName, namespaceId, ex);
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
            await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Values[EncodeKeyName(keyName)].PutAsync(multipartBody, requestConfig, cancellationToken).NoSync();
        }
        catch (WorkersKvApiResponseCommonFailure ex)
        {
            _logger.LogError(ex, "Failed to put KV value for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
            throw CreateFailureException("put", keyName, namespaceId, ex);
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
            await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Values[EncodeKeyName(keyName)].DeleteAsync(body, null, cancellationToken).NoSync();
        }
        catch (WorkersKvApiResponseCommonFailure ex) when (ex.ResponseStatusCode == 404)
        {
            _logger.LogDebug("KV key {KeyName} in namespace {NamespaceId} was already absent", keyName, namespaceId);
        }
        catch (WorkersKvApiResponseCommonFailure ex)
        {
            _logger.LogError(ex, "Failed to delete KV key {KeyName} from namespace {NamespaceId}", keyName, namespaceId);
            throw CreateFailureException("delete", keyName, namespaceId, ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete KV key {KeyName} from namespace {NamespaceId}", keyName, namespaceId);
            throw;
        }
    }

    public async ValueTask<WorkersKvNamespaceListANamespaceSKeys200?> ListKeys(string accountId, string namespaceId, string? prefix = null, int? limit = null, string? cursor = null, CancellationToken cancellationToken = default)
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

    public async ValueTask<WorkersKvNamespaceReadTheMetadataForAKey200?> GetKeyMetadata(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentException.ThrowIfNullOrEmpty(keyName);
        _logger.LogDebug("Getting KV metadata for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        try
        {
            return await client.Accounts[accountId].Storage.Kv.Namespaces[namespaceId].Metadata[EncodeKeyName(keyName)].GetAsync(null, cancellationToken).NoSync();
        }
        catch (WorkersKvApiResponseCommonFailure ex) when (ex.ResponseStatusCode == 404)
        {
            _logger.LogDebug("KV metadata for key {KeyName} in namespace {NamespaceId} was not found", keyName, namespaceId);
            return null;
        }
        catch (WorkersKvApiResponseCommonFailure ex)
        {
            _logger.LogError(ex, "Failed to get KV metadata for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
            throw CreateFailureException("get metadata for", keyName, namespaceId, ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get KV metadata for key {KeyName} in namespace {NamespaceId}", keyName, namespaceId);
            throw;
        }
    }

    public async ValueTask<WorkersKvNamespaceGetMultipleKeyValuePairs200?> BulkGet(string accountId, string namespaceId, IReadOnlyList<string> keys, bool withMetadata = false, WorkersKvNamespaceGetMultipleKeyValuePairs_type? type = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count > 100) throw new ArgumentException("Maximum 100 keys allowed for bulk get.", nameof(keys));
        _logger.LogDebug("Bulk getting {Count} KV keys from namespace {NamespaceId}", keys.Count, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new WorkersKvNamespaceGetMultipleKeyValuePairs
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

    public async ValueTask<WorkersKvNamespaceWriteMultipleKeyValuePairs200?> BulkPut(string accountId, string namespaceId, IReadOnlyList<WorkersKvBulkWriteItem> pairs, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(accountId);
        ArgumentException.ThrowIfNullOrEmpty(namespaceId);
        ArgumentNullException.ThrowIfNull(pairs);
        if (pairs.Count > 10000) throw new ArgumentException("Maximum 10,000 pairs allowed for bulk put.", nameof(pairs));
        _logger.LogDebug("Bulk putting {Count} KV pairs to namespace {NamespaceId}", pairs.Count, namespaceId);
        CloudflareOpenApiClient client = await _clientUtil.Get(cancellationToken).NoSync();
        var body = new List<WorkersKvBulkWriteItem>(pairs);
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

    public async ValueTask<WorkersKvNamespaceDeleteMultipleKeyValuePairs200?> BulkDelete(string accountId, string namespaceId, IReadOnlyList<string> keys, CancellationToken cancellationToken = default)
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

    private static string EncodeKeyName(string keyName)
    {
        return Uri.EscapeDataString(keyName);
    }

    private static InvalidOperationException CreateFailureException(string operation, string keyName, string namespaceId, WorkersKvApiResponseCommonFailure exception)
    {
        return new InvalidOperationException(
            $"Failed to {operation} KV key {keyName} in namespace {namespaceId}. Status: {exception.ResponseStatusCode}. {GetFailureMessage(exception)}",
            exception);
    }

    private static string GetFailureMessage(WorkersKvApiResponseCommonFailure exception)
    {
        List<WorkersKvMessagesItem>? errors = exception.Errors?.Value;

        if (errors == null || errors.Count == 0)
            return exception.Message;

        return string.Join("; ", errors.Select(error =>
        {
            if (error.Code.HasValue && !string.IsNullOrWhiteSpace(error.Message))
                return $"{error.Code}: {error.Message}";

            if (error.Code.HasValue)
                return error.Code.Value.ToString();

            return error.Message ?? exception.Message;
        }));
    }
}
