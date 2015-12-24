using System;

using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockerTesterConfig
    {
        public int? LockCreatorsCount { get; set; }
        public LocalRivalOptimization? LocalRivalOptimization { get; set; }
        public TimeSpan? LockTtl { get; set; }
        public TimeSpan? KeepLockAliveInterval { get; set; }
        public ICassandraClusterSettings CassandraClusterSettings { get; set; }
        public TimestampProviderStochasticType StochasticType { get; set; }
    }

    public enum TimestampProviderStochasticType
    {
        OnlyPositive,
        BothPositiveAndNegative
    }
}