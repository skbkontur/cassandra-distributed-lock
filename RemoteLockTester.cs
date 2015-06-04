using System;
using System.Linq;

using GroBuf;
using GroBuf.DataMembersExtracters;

using Metrics;
using Metrics.Reporters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockTester : IDisposable, IRemoteLockCreator
    {
        public RemoteLockTester(RemoteLockTesterConfig config = null)
        {
            config = config ?? new RemoteLockTesterConfig();
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
            remoteLockLocalManagers = new RemoteLockLocalManager[lockCreatorsCount];
            remoteLockCreators = new RemoteLockCreator[lockCreatorsCount];
            switch(config.LocalRivalOptimization)
            {
            case null:
            case LocalRivalOptimization.Enabled:
                var remoteLockLocalManager = new RemoteLockLocalManager(remoteLockImplementation);
                var remoteLockCreator = new RemoteLockCreator(remoteLockLocalManager);
                for(var i = 0; i < lockCreatorsCount; i++)
                {
                    remoteLockLocalManagers[i] = remoteLockLocalManager;
                    remoteLockCreators[i] = remoteLockCreator;
                }
                break;
            case LocalRivalOptimization.Disabled:
                for(var i = 0; i < lockCreatorsCount; i++)
                {
                    remoteLockLocalManagers[i] = new RemoteLockLocalManager(remoteLockImplementation);
                    remoteLockCreators[i] = new RemoteLockCreator(remoteLockLocalManagers[i]);
                }
                break;
            default:
                throw new InvalidOperationException(string.Format("Invalid localRivalOptimization: {0}", config.LocalRivalOptimization));
            }
        }

        public void Dispose()
        {
            foreach(var remoteLockLocalManager in remoteLockLocalManagers)
                remoteLockLocalManager.Dispose();
            LogRemoteLockerPerfStat();
        }

        private static void LogRemoteLockerPerfStat()
        {
            var metricsData = Metric.Context("RemoteLocker").DataProvider.CurrentMetricsData;
            var metricsReport = StringReport.RenderMetrics(metricsData, () => new HealthStatus());
            Console.Out.WriteLine(metricsReport);
        }

        public IRemoteLockCreator this[int index] { get { return remoteLockCreators[index]; } }

        public IRemoteLock Lock(string lockId)
        {
            return remoteLockCreators.Single().Lock(lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            return remoteLockCreators.Single().TryGetLock(lockId, out remoteLock);
        }

        private readonly RemoteLockCreator[] remoteLockCreators;
        private readonly RemoteLockLocalManager[] remoteLockLocalManagers;
    }
}