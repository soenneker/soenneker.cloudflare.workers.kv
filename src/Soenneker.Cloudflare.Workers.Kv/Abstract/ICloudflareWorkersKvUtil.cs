using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Cloudflare.OpenApiClient.Models;

namespace Soenneker.Cloudflare.Workers.Kv.Abstract;

/// <summary>
/// A utility for managing Cloudflare Workers KV data via the Cloudflare API
/// </summary>
public interface ICloudflareWorkersKvUtil
{
    // Namespaces
    /// <summary>
    /// Lists all KV namespaces for the given account.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<Workers_kv_namespace_list_namespaces_200?> ListNamespaces(string accountId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a KV namespace with the given title.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="title">Human-readable name for the namespace.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<Workers_kv_namespace_create_a_namespace_200?> CreateNamespace(string accountId, string title, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a namespace by ID.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="namespaceId">The namespace ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<Workers_kv_namespace_get_a_namespace_200?> GetNamespace(string accountId, string namespaceId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Renames a namespace.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="namespaceId">The namespace ID.</param>
    /// <param name="title">New human-readable name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<Workers_kv_namespace_rename_a_namespace_200?> RenameNamespace(string accountId, string namespaceId, string title, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a namespace.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="namespaceId">The namespace ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask DeleteNamespace(string accountId, string namespaceId, CancellationToken cancellationToken = default);

    // Key-value operations
    /// <summary>
    /// Reads the value for a key. Returns null if the key does not exist.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="namespaceId">The KV namespace ID.</param>
    /// <param name="keyName">The key name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<Stream?> GetValue(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the value for a key as a string. Returns null if the key does not exist.
    /// </summary>
    ValueTask<string?> GetValueAsString(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes a key-value pair. Overwrites existing value and metadata.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="namespaceId">The KV namespace ID.</param>
    /// <param name="keyName">The key name.</param>
    /// <param name="value">The value to store (UTF-8 string).</param>
    /// <param name="expirationTtlSeconds">Optional TTL in seconds (minimum 60).</param>
    /// <param name="expirationUnixSeconds">Optional absolute expiration (Unix timestamp).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask PutValue(string accountId, string namespaceId, string keyName, string value, int? expirationTtlSeconds = null, long? expirationUnixSeconds = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a key from the namespace.
    /// </summary>
    ValueTask DeleteKey(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists keys in a namespace with optional prefix and cursor.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="namespaceId">The KV namespace ID.</param>
    /// <param name="prefix">Optional prefix to filter keys.</param>
    /// <param name="limit">Max keys to return (default 100).</param>
    /// <param name="cursor">Pagination cursor from previous response.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<Workers_kv_namespace_list_a_namespace_s_keys_200?> ListKeys(string accountId, string namespaceId, string? prefix = null, int? limit = null, string? cursor = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads metadata for a key (no value).
    /// </summary>
    ValueTask<Workers_kv_namespace_read_the_metadata_for_a_key_200?> GetKeyMetadata(string accountId, string namespaceId, string keyName, CancellationToken cancellationToken = default);

    // Bulk operations
    /// <summary>
    /// Retrieves up to 100 key-value pairs. Keys must be text-based.
    /// </summary>
    /// <param name="accountId">The Cloudflare account ID.</param>
    /// <param name="namespaceId">The KV namespace ID.</param>
    /// <param name="keys">List of keys (max 100).</param>
    /// <param name="withMetadata">Include metadata in response.</param>
    /// <param name="type">Text or Json for value parsing.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<Workers_kv_namespace_get_multiple_key_value_pairs_200?> BulkGet(string accountId, string namespaceId, IReadOnlyList<string> keys, bool withMetadata = false, Workers_kv_namespace_get_multiple_key_value_pairs_type? type = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes multiple key-value pairs (up to 10,000). Request size must be 100 MB or less.
    /// </summary>
    ValueTask<Workers_kv_namespace_write_multiple_key_value_pairs_200?> BulkPut(string accountId, string namespaceId, IReadOnlyList<Workers_kv_bulk_write_item> pairs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes multiple keys (up to 10,000).
    /// </summary>
    ValueTask<Workers_kv_namespace_delete_multiple_key_value_pairs_200?> BulkDelete(string accountId, string namespaceId, IReadOnlyList<string> keys, CancellationToken cancellationToken = default);
}
