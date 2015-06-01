using System;
using System.Net;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockTester : IDisposable, IRemoteLockCreator
    {
        public RemoteLockTester(ICassandraClusterSettings cassandraClusterSettings = null, TimeSpan? lockTtl = null, TimeSpan? keepLockAliveInterval = null)
        {
            cassandraClusterSettings = cassandraClusterSettings ?? StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback);
            var cassandraCluster = new CassandraCluster(cassandraClusterSettings);
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var cassandraRemoteLockImplementationSettings = new CassandraRemoteLockImplementationSettings
                {
                    ColumnFamilyFullName = ColumnFamilies.remoteLock,
                    LockTtl = lockTtl ?? TimeSpan.FromSeconds(10),
                    KeepLockAliveInterval = keepLockAliveInterval ?? TimeSpan.FromSeconds(2),
                };
            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, cassandraRemoteLockImplementationSettings);
            remoteLockLocalManager = new RemoteLockLocalManager(remoteLockImplementation);
            remoteLockCreator = new RemoteLockCreator(remoteLockLocalManager);
        }

        public void Dispose()
        {
            remoteLockLocalManager.Dispose();
        }

        public IRemoteLock Lock(string lockId)
        {
            return remoteLockCreator.Lock(lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            return remoteLockCreator.TryGetLock(lockId, out remoteLock);
        }

        private readonly RemoteLockCreator remoteLockCreator;
        private readonly RemoteLockLocalManager remoteLockLocalManager;
    }
}