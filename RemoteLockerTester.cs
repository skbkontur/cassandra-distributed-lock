using System;
using System.Linq;

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
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var cassandraCluster = new CassandraCluster(config.CassandraClusterSettings ?? CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node));
            var cassandraRemoteLockImplementationSettings = new CassandraRemoteLockImplementationSettings
                {
                    ColumnFamilyFullName = ColumnFamilies.remoteLock,
                    LockTtl = config.LockTtl ?? TimeSpan.FromSeconds(10),
                    KeepLockAliveInterval = config.KeepLockAliveInterval ?? TimeSpan.FromSeconds(2),
                };
            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, cassandraRemoteLockImplementationSettings);
            var lockCreatorsCount = config.LockCreatorsCount ?? 1;
            remoteLockers = new RemoteLocker[lockCreatorsCount];
            switch(config.LocalRivalOptimization)
            {
            case null:
            case LocalRivalOptimization.Enabled:
                var remoteLocker = new RemoteLocker(remoteLockImplementation);
                for(var i = 0; i < lockCreatorsCount; i++)
                    remoteLockers[i] = remoteLocker;
                break;
            case LocalRivalOptimization.Disabled:
                for(var i = 0; i < lockCreatorsCount; i++)
                    remoteLockers[i] = new RemoteLocker(remoteLockImplementation);
                break;
            default:
                throw new InvalidOperationException(string.Format("Invalid localRivalOptimization: {0}", config.LocalRivalOptimization));
            }
        }

        public void Dispose()
        {
            foreach(var remoteLockLocalManager in remoteLockers)
                remoteLockLocalManager.Dispose();
            LogRemoteLockerPerfStat();
        }

        private static void LogRemoteLockerPerfStat()
        {
            var metricsData = Metric.Context("RemoteLocker").DataProvider.CurrentMetricsData;
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

        private readonly RemoteLocker[] remoteLockers;
    }
}