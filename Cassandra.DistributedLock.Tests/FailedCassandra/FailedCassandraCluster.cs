using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Clusters.ActualizationEventListener;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Core.Pools;
using SkbKontur.Cassandra.ThriftClient.Scheme;

namespace Cassandra.DistributedLock.Tests.FailedCassandra
{
    public class FailedCassandraCluster : ICassandraCluster
    {
        public FailedCassandraCluster(ICassandraCluster cassandraCluster, double failProbability)
        {
            this.cassandraCluster = cassandraCluster;
            this.failProbability = failProbability;
        }

        public void Dispose()
        {
            cassandraCluster.Dispose();
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return cassandraCluster.RetrieveClusterConnection();
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName)
        {
            return cassandraCluster.RetrieveKeyspaceConnection(keyspaceName);
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            var columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
            return new FailedColumnFamilyConnection(columnFamilyConnection, failProbability);
        }

        public IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName)
        {
            throw new NotImplementedException();
        }

        public ITimeBasedColumnFamilyConnection RetrieveTimeBasedColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            throw new NotImplementedException();
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            return cassandraCluster.GetKnowledges();
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaces, ICassandraActualizerEventListener eventListener = null, bool changeExistingKeyspaceMetadata = false)
        {
            cassandraCluster.ActualizeKeyspaces(keyspaces, eventListener, changeExistingKeyspaceMetadata);
        }

        private readonly double failProbability;
        private readonly ICassandraCluster cassandraCluster;
    }
}