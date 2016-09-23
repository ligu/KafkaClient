using System;

namespace KafkaClient.Common
{
    public interface ILog
    {
        /// <summary>
        /// Record debug information using the String.Format syntax.
        /// </summary>
        void DebugFormat(string format, params object[] args);

        /// <summary>
        /// Record debug information using the String.Format syntax.
        /// </summary>
        void DebugFormat(Exception exception, string format = null, params object[] args);

        /// <summary>
        /// Record info information using the String.Format syntax.
        /// </summary>
        void InfoFormat(string format, params object[] args);

        /// <summary>
        /// Record info information using the String.Format syntax.
        /// </summary>
        void InfoFormat(Exception exception, string format = null, params object[] args);

        /// <summary>
        /// Record warning information using the String.Format syntax.
        /// </summary>
        void WarnFormat(string format, params object[] args);

        /// <summary>
        /// Record warning information using the String.Format syntax.
        /// </summary>
        void WarnFormat(Exception exception, string format = null, params object[] args);

        /// <summary>
        /// Record error information using the String.Format syntax.
        /// </summary>
        void ErrorFormat(string format, params object[] args);

        /// <summary>
        /// Record error information using the String.Format syntax.
        /// </summary>
        void ErrorFormat(Exception exception, string format = null, params object[] args);
    }
}