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
        public RemoteLockerTester(bool useSingleLockKeeperThread = true, RemoteLockerTesterConfig config = null)
        {
            config = config ?? new RemoteLockerTesterConfig();
            this.useSingleLockKeeperThread = useSingleLockKeeperThread;
            localRivalOptimizationIsEnabled = config.LocalRivalOptimization != LocalRivalOptimization.Disabled;
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var cassandraCluster = new CassandraCluster(config.CassandraClusterSettings ?? CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node));
            var cassandraRemoteLockImplementationSettings = new CassandraRemoteLockImplementationSettings(
                ColumnFamilies.remoteLock, config.LockTtl ?? TimeSpan.FromSeconds(10), config.KeepLockAliveInterval ?? TimeSpan.FromSeconds(2), 10);
                
            cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, cassandraRemoteLockImplementationSettings);
            var lockCreatorsCount = config.LockCreatorsCount ?? 1;
            remoteLockers = new RemoteLocker[lockCreatorsCount];
            if(useSingleLockKeeperThread)
            {
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
                        remoteLockers[i] = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics);
                }
            }
            else
                remoteLockCreator = new RemoteLockCreator(cassandraRemoteLockImplementation);
        }

        public void Dispose()
        {
            if(useSingleLockKeeperThread)
            {
                foreach(var remoteLockLocalManager in remoteLockers)
                    remoteLockLocalManager.Dispose();
                LogRemoteLockerPerfStat();
            }
        }

        private void LogRemoteLockerPerfStat()
        {
            var metricsData = remoteLockerMetrics.Context.DataProvider.CurrentMetricsData;
            var metricsReport = StringReport.RenderMetrics(metricsData, () => new HealthStatus());
            Console.Out.WriteLine(metricsReport);
        }

        public IRemoteLockCreator this[int index] { get { return GetRemoteLockCreator(remoteLockers[index]); } }

        public IRemoteLock Lock(string lockId)
        {
            return GetRemoteLockCreator(remoteLockers.Single()).Lock(lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            return GetRemoteLockCreator(remoteLockers.Single()).TryGetLock(lockId, out remoteLock);
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

        private IRemoteLockCreator GetRemoteLockCreator(RemoteLocker remoteLocker)
        {
            if(useSingleLockKeeperThread)
                return remoteLocker;
            if(localRivalOptimizationIsEnabled)
                return remoteLockCreator;
            return new RemoteLockCreatorWthoutLocalRivalOptimization(remoteLockCreator);
        }

        private readonly bool useSingleLockKeeperThread;
        private readonly bool localRivalOptimizationIsEnabled;
        private readonly RemoteLocker[] remoteLockers;
        private readonly RemoteLockCreator remoteLockCreator;
        private readonly RemoteLockerMetrics remoteLockerMetrics;
        private readonly CassandraRemoteLockImplementation cassandraRemoteLockImplementation;
    }
}