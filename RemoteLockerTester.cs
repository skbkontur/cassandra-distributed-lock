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
            var cassandraRemoteLockImplementationSettings = new CassandraRemoteLockImplementationSettings
                {
                    ColumnFamilyFullName = ColumnFamilies.remoteLock,
                    LockTtl = config.LockTtl ?? TimeSpan.FromSeconds(10),
                    KeepLockAliveInterval = config.KeepLockAliveInterval ?? TimeSpan.FromSeconds(2),
                };
            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, cassandraRemoteLockImplementationSettings);
            var lockCreatorsCount = config.LockCreatorsCount ?? 1;
            remoteLockers = new RemoteLocker[lockCreatorsCount];
            if(useSingleLockKeeperThread)
            {
                if(localRivalOptimizationIsEnabled)
                {
                    var remoteLocker = new RemoteLocker(remoteLockImplementation);
                    for(var i = 0; i < lockCreatorsCount; i++)
                        remoteLockers[i] = remoteLocker;
                }
                else
                {
                    for(var i = 0; i < lockCreatorsCount; i++)
                        remoteLockers[i] = new RemoteLocker(remoteLockImplementation);
                }
            }
            else
                remoteLockCreator = new RemoteLockCreator(remoteLockImplementation);
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

        private static void LogRemoteLockerPerfStat()
        {
            var metricsData = RemoteLockerMetrics.Context.DataProvider.CurrentMetricsData;
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
    }
}