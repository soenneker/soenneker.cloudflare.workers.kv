using Soenneker.Cloudflare.Workers.Kv.Abstract;
using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.Cloudflare.Workers.Kv.Tests;

[Collection("Collection")]
public sealed class CloudflareWorkersKvUtilTests : FixturedUnitTest
{
    private readonly ICloudflareWorkersKvUtil _util;

    public CloudflareWorkersKvUtilTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<ICloudflareWorkersKvUtil>(true);
    }

    [Fact]
    public void Default()
    {

    }
}
