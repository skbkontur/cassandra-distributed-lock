namespace Cassandra.DistributedLock.Tests
{
    public enum TimestampProviderStochasticType
    {
        None,
        OnlyPositive,
        BothPositiveAndNegative
    }
}