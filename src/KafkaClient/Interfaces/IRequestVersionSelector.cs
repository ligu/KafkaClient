using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public interface IRequestVersionSelector
    {
        /// <summary>
        /// Force a call to the kafka servers to refresh supported version information.
        /// </summary>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers to refresh supported version information.
        /// Only call this method to force an update. For all other queries use <see cref="GetSupportedVersion"/> which uses cached values.
        /// </remarks>
        Task RefreshSupportedVersionsAsync();

        /// <summary>
        /// Get the (cached) version support for the given api. Will return null if version support is unknown.
        /// </summary>
        /// <param name="apiKey">The api under consideration.</param>
        /// <returns></returns>
        ApiVersionSupport GetSupportedVersion(ApiKeyRequestType apiKey);
    }
}