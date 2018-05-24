using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace Cassandra.DistributedLock.Tests
{
    public class CassandraSchemeActualizer
    {
        public CassandraSchemeActualizer(ICassandraCluster cassandraCluster)
        {
            this.cassandraCluster = cassandraCluster;
        }

        public void AddNewColumnFamilies()
        {
            cassandraCluster.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
                        {
                            Name = TestConsts.RemoteLockKeyspace,
                            Configuration = new KeyspaceConfiguration
                                {
                                    ReplicationStrategy = SimpleReplicationStrategy.Create(replicationFactor : 1),
                                    ColumnFamilies = new[]
                                        {
                                            new ColumnFamily
                                                {
                                                    Name = TestConsts.RemoteLockColumnFamily,
                                                    Caching = ColumnFamilyCaching.KeysOnly
                                                }
                                        },
                                },
                        }
                });
        }

        public void TruncateAllColumnFamilies()
        {
            cassandraCluster.RetrieveColumnFamilyConnection(TestConsts.RemoteLockKeyspace, TestConsts.RemoteLockColumnFamily).Truncate();
        }

        private readonly ICassandraCluster cassandraCluster;
    }
}