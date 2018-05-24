using System;

using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace Cassandra.DistributedLock.Tests
{
    public class RemoteLockerTesterConfig
    {
        public int LockersCount { get; set; }
        public LocalRivalOptimization LocalRivalOptimization { get; set; }
        public TimeSpan LockTtl { get; set; }
        public TimeSpan LockMetadataTtl { get; set; }
        public TimeSpan KeepLockAliveInterval { get; set; }
        public int ChangeLockRowThreshold { get; set; }
        public TimestampProviderStochasticType TimestamProviderStochasticType { get; set; }
        public ICassandraClusterSettings CassandraClusterSettings { get; set; }
        public double? CassandraFailProbability { get; set; }

        public static RemoteLockerTesterConfig Default()
        {
            return new RemoteLockerTesterConfig
                {
                    LockersCount = 1,
                    LocalRivalOptimization = LocalRivalOptimization.Enabled,
                    LockTtl = TimeSpan.FromSeconds(10),
                    LockMetadataTtl = TimeSpan.FromMinutes(1),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(2),
                    ChangeLockRowThreshold = 10,
                    TimestamProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraClusterSettings = SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(),
                    CassandraFailProbability = null,
                };
        }
    }
}