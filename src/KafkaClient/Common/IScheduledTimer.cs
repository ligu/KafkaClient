using System;

namespace KafkaNet.Common
{
    public interface IScheduledTimer : IDisposable
    {
        /// <summary>
        ///     Current running status of the timer.
        /// </summary>
        ScheduledTimerStatus Status { get; }

        /// <summary>
        /// Indicates if the timer is running.
        /// </summary>
        bool Enabled { get; }

        /// <summary>
        ///     Set the time to start a replication.
        /// </summary>
        /// <param name="start">Start date and time for the replication timer.</param>
        /// <returns>Instance of ScheduledTimer for fluent configuration.</returns>
        /// <remarks>If no interval is set, the replication will only happen once.</remarks>
        IScheduledTimer StartingAt(DateTime start);

        /// <summary>
        ///     Set the interval to send a replication command to a Solr server.
        /// </summary>
        /// <param name="interval">Interval this command is to be called.</param>
        /// <returns>Instance of ScheduledTimer for fluent configuration.</returns>
        /// <remarks>If no start time is set, the interval starts when the timer is started.</remarks>
        IScheduledTimer Every(TimeSpan interval);

        /// <summary>
        ///     Action to perform when the timer expires.
        /// </summary>
        IScheduledTimer Do(Action action);

        /// <summary>
        /// Sets the timer to execute and restart the timer without waiting for the Do method to finish.
        /// </summary>
        /// <returns></returns>
        IScheduledTimer DontWait();

        /// <summary>
        ///     Starts the timer
        /// </summary>
        IScheduledTimer Begin();

        /// <summary>
        ///     Stop the timer.
        /// </summary>
        IScheduledTimer End();
    }
}