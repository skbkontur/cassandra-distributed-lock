using System;
using System.Linq;
using System.Threading.Tasks;

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
            config = config ?? RemoteLockerTesterConfig.Default();
            var localRivalOptimizationIsEnabled = config.LocalRivalOptimization != LocalRivalOptimization.Disabled;
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var cassandraCluster = new CassandraCluster(config.CassandraClusterSettings);
            var timestampProvider = new StochasticTimestampProvider(config.TimestamProviderStochasticType, config.LockTtl);
            var implementationSettings = new CassandraRemoteLockImplementationSettings(timestampProvider, ColumnFamilies.remoteLock, config.LockTtl, config.KeepLockAliveInterval, config.ChangeLockRowThreshold);
            cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings);
            remoteLockers = new RemoteLocker[config.LockersCount];
            remoteLockerMetrics = new RemoteLockerMetrics("dummyKeyspace");
            if(localRivalOptimizationIsEnabled)
            {
                var remoteLocker = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics);
                for(var i = 0; i < config.LockersCount; i++)
                    remoteLockers[i] = remoteLocker;
            }
            else
            {
                for(var i = 0; i < config.LockersCount; i++)
                    remoteLockers[i] = new RemoteLocker(new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings), remoteLockerMetrics);
            }
        }

        public void Dispose()
        {
            var disposeTasks = new Task[remoteLockers.Length];
            for(var i = 0; i < remoteLockers.Length; i++)
            {
                var localI = i;
                disposeTasks[i] = Task.Factory.StartNew(() => remoteLockers[localI].Dispose());
            }
            Task.WaitAll(disposeTasks);
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
    }
}