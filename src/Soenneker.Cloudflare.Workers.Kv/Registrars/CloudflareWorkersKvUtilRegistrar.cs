using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.Cloudflare.Utils.Client.Registrars;
using Soenneker.Cloudflare.Workers.Kv.Abstract;

namespace Soenneker.Cloudflare.Workers.Kv.Registrars;

/// <summary>
/// A utility for managing Cloudflare Workers KV data
/// </summary>
public static class CloudflareWorkersKvUtilRegistrar
{
    /// <summary>
    /// Adds <see cref="ICloudflareWorkersKvUtil"/> as a singleton service. <para/>
    /// </summary>
    public static IServiceCollection AddCloudflareWorkersKvUtilAsSingleton(this IServiceCollection services)
    {
        services.AddCloudflareClientUtilAsSingleton().TryAddSingleton<ICloudflareWorkersKvUtil, CloudflareWorkersKvUtil>();

        return services;
    }

    /// <summary>
    /// Adds <see cref="ICloudflareWorkersKvUtil"/> as a scoped service. <para/>
    /// </summary>
    public static IServiceCollection AddCloudflareWorkersKvUtilAsScoped(this IServiceCollection services)
    {
        services.AddCloudflareClientUtilAsSingleton().TryAddScoped<ICloudflareWorkersKvUtil, CloudflareWorkersKvUtil>();

        return services;
    }
}
