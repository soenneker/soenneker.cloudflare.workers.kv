using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Soenneker.Cloudflare.Workers.Kv.Abstract;
using Soenneker.Tests.HostedUnit;
using System;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Extensions.Configuration;

namespace Soenneker.Cloudflare.Workers.Kv.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class CloudflareWorkersKvUtilTests : HostedUnitTest
{
    private readonly ICloudflareWorkersKvUtil _util;
    private readonly IConfiguration _config;

    public CloudflareWorkersKvUtilTests(Host host) : base(host)
    {
        _util = Resolve<ICloudflareWorkersKvUtil>(true);
        _config = Resolve<IConfiguration>();
    }

    [Test]
    public void Default()
    {
    }

    [Test]
    [Skip("Manual")]
    public async ValueTask Set_and_read_value(CancellationToken cancellationToken)
    {
        string accountId = _config.GetStringStrict("Cloudflare:AccountId");
        string apiKey = _config.GetStringStrict("Cloudflare:ApiKey");
        string namespaceId = _config.GetStringStrict("Cloudflare:WorkersKv:NamespaceId");
        string key = $"workers-kv-test-{Guid.NewGuid():N}";
        const string value = "test-value";

        try
        {
            await _util.PutValue(accountId, apiKey, namespaceId, key, value, cancellationToken: cancellationToken);

            string? result = await _util.GetValueAsString(accountId, apiKey, namespaceId, key, cancellationToken);

            result.Should().Be(value);
        }
        finally
        {
            await _util.DeleteKey(accountId, apiKey, namespaceId, key, cancellationToken);
        }
    }
}