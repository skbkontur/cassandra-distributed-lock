using System;
using System.Collections.Generic;

using GroboContainer.Infection;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Clusters.ActualizationEventListener;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class FailedCassanraClusterException : Exception
    {
        public FailedCassanraClusterException(string message)
            : base(message)
        {
        }
    }

    [IgnoredImplementation]
    public class FailedCassandraCluster : ICassandraCluster
    {
        public FailedCassandraCluster(Func<ICassandraCluster> getCassandraCluster)
        {
            this.getCassandraCluster = getCassandraCluster;
        }

        public void Dispose()
        {
            CassandraCluster.Dispose();
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return CassandraCluster.RetrieveClusterConnection();
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName)
        {
            return CassandraCluster.RetrieveKeyspaceConnection(keyspaceName);
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            if(random == null)
                random = new Random();
            if(random.Next(30) == 0)
                throw new FailedCassanraClusterException("Ошибка при работе с Кассандрой");
            return CassandraCluster.RetrieveColumnFamilyConnection(keySpaceName, columnFamilyName);
        }

        public IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName)
        {
            throw new NotImplementedException();
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            return CassandraCluster.GetKnowledges();
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaces, ICassandraActualizerEventListener eventListener = null)
        {
            CassandraCluster.ActualizeKeyspaces(keyspaces);
        }

        [ThreadStatic]
        public static Random random;

        private ICassandraCluster CassandraCluster
        {
            get
            {
                if(cassandraCluster == null)
                    return cassandraCluster = getCassandraCluster();
                return cassandraCluster;
            }
        }

        private readonly Func<ICassandraCluster> getCassandraCluster;
        private ICassandraCluster cassandraCluster;
    }
}