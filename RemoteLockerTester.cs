using System;
using System.Linq;

using GroboContainer.Infection;

using GroBuf;
using GroBuf.DataMembersExtracters;

using Metrics;
using Metrics.Reporters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockerTester : IDisposable, IRemoteLockCreator
    {
        public RemoteLockerTester(RemoteLockerTesterConfig config = null)
        {
            config = config ?? new RemoteLockerTesterConfig();
            var localRivalOptimizationIsEnabled = config.LocalRivalOptimization != LocalRivalOptimization.Disabled;
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var cassandraCluster = new CassandraCluster(config.CassandraClusterSettings ?? CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node));
            var cassandraRemoteLockImplementationSettings = new CassandraRemoteLockImplementationSettings(
                ColumnFamilies.remoteLock, config.LockTtl ?? TimeSpan.FromSeconds(10), config.KeepLockAliveInterval ?? TimeSpan.FromSeconds(2), 2);

            cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, cassandraRemoteLockImplementationSettings);
            var lockCreatorsCount = config.LockCreatorsCount ?? 1;
            remoteLockers = new RemoteLocker[lockCreatorsCount];
            remoteLockerMetrics = new RemoteLockerMetrics("dummyKeyspace");
            if(localRivalOptimizationIsEnabled)
            {
                var remoteLocker = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics);
                for(var i = 0; i < lockCreatorsCount; i++)
                    remoteLockers[i] = remoteLocker;
            }
            else
            {
                for(var i = 0; i < lockCreatorsCount; i++)
                    remoteLockers[i] = new RemoteLocker(new CassandraRemoteLockImplementation(cassandraCluster, serializer, cassandraRemoteLockImplementationSettings, new StochasticTimestampProvider(config.StochasticType)), remoteLockerMetrics);
            }
        }

        public void Dispose()
        {
            foreach(var remoteLockLocalManager in remoteLockers)
                remoteLockLocalManager.Dispose();
            LogRemoteLockerPerfStat();
        }

        private void LogRemoteLockerPerfStat()
        {
            var metricsData = remoteLockerMetrics.Context.DataProvider.CurrentMetricsData;
            var metricsReport = StringReport.RenderMetrics(metricsData, () => new HealthStatus());
            Console.Out.WriteLine(metricsReport);
        }

        public IRemoteLockCreator this[int index] { get { return remoteLockers[index]; } }

        public IRemoteLock Lock(string lockId)
        {
            return remoteLockers.Single().Lock(lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            return remoteLockers.Single().TryGetLock(lockId, out remoteLock);
        }

        public string[] GetThreadsInMainRow(string lockId)
        {
            return cassandraRemoteLockImplementation.GetLockThreads(lockId);
        }

        public string[] GetThreadsInShadeRow(string lockId)
        {
            return cassandraRemoteLockImplementation.GetShadeThreads(lockId);
        }

        public LockMetadata GetLockMetadata(string lockId)
        {
            return cassandraRemoteLockImplementation.GetLockMetadata(lockId);
        }

        private readonly RemoteLocker[] remoteLockers;
        private readonly RemoteLockerMetrics remoteLockerMetrics;
        private readonly CassandraRemoteLockImplementation cassandraRemoteLockImplementation;

        [IgnoredImplementation]
        private class StochasticTimestampProvider : ITimestampProvider
        {
            private readonly TimestampProviderStochasticType stochasticType;

            public StochasticTimestampProvider(TimestampProviderStochasticType stochasticType)
            {
                this.stochasticType = stochasticType;
            }

            public long GetNowTicks()
            {
                var diff = TimeSpan.FromSeconds(Rng.Next(50, 100)).Ticks;
                if(stochasticType == TimestampProviderStochasticType.BothPositiveAndNegative)
                    diff *= Rng.Next(-1, 2);
                return DateTime.UtcNow.Ticks + diff;
            }

            [ThreadStatic]
            private static Random random;

            private static Random Rng { get { return random ?? (random = new Random(Guid.NewGuid().GetHashCode())); } }
        }
    }
}