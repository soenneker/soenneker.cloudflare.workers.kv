using Soenneker.Cloudflare.Workers.Kv.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.Cloudflare.Workers.Kv.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class CloudflareWorkersKvUtilTests : HostedUnitTest
{
    private readonly ICloudflareWorkersKvUtil _util;

    public CloudflareWorkersKvUtilTests(Host host) : base(host)
    {
        _util = Resolve<ICloudflareWorkersKvUtil>(true);
    }

    [Test]
    public void Default()
    {

    }
}
