namespace KafkaClient.Common
{
    public class RetryAttempt<T>
    {
        private static RetryAttempt<T> _failed;
        public static RetryAttempt<T> Failed => _failed ?? (_failed = new RetryAttempt<T>(default(T), false));

        public RetryAttempt(T value, bool isSuccessful = true)
        {
            Value = value;
            IsSuccessful = isSuccessful;
        }

        public bool IsSuccessful { get; }
        public T Value { get; }
    }
}