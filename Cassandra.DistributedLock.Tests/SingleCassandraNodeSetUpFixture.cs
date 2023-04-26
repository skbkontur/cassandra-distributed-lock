using System;
using System.Linq;
using System.Net;

using Cassandra.DistributedLock.Tests.Logging;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Schema;

namespace Cassandra.DistributedLock.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        [OneTimeSetUp]
        public static void SetUp()
        {
            Log4NetConfiguration.InitializeOnce();

            var logger = Log4NetConfiguration.RootLogger.ForContext(nameof(SingleCassandraNodeSetUpFixture));
            cassandraCluster = new CassandraCluster(CreateCassandraClusterSettings(), logger);

            var cassandraSchemaActualizer = new CassandraSchemaActualizer(cassandraCluster, eventListener : null, logger);
            cassandraSchemaActualizer.ActualizeKeyspaces(new[]
                {
                    new KeyspaceSchema
                        {
                            Name = RemoteLockKeyspace,
                            Configuration = new KeyspaceConfiguration
                                {
                                    ReplicationStrategy = SimpleReplicationStrategy.Create(replicationFactor : 1),
                                    ColumnFamilies = new[]
                                        {
                                            new ColumnFamily
                                                {
                                                    Name = RemoteLockColumnFamily,
                                                    Caching = ColumnFamilyCaching.KeysOnly
                                                }
                                        }
                                }
                        }
                }, changeExistingKeyspaceMetadata : false);
        }

        public static ICassandraClusterSettings CreateCassandraClusterSettings(int attempts = 5, TimeSpan? timeout = null)
        {
            return new SingleNodeCassandraClusterSettings(new IPEndPoint(GetIpV4Address("127.0.0.1"), 9160))
                {
                    ClusterName = "TestCluster",
                    Attempts = attempts,
                    Timeout = (int)(timeout ?? TimeSpan.FromSeconds(6)).TotalMilliseconds,
                };
        }

        private static IPAddress GetIpV4Address(string hostNameOrIpAddress)
        {
            if (IPAddress.TryParse(hostNameOrIpAddress, out var res))
                return res;

            return Dns.GetHostEntry(hostNameOrIpAddress).AddressList.First(address => !address.ToString().Contains(':'));
        }

        public static void TruncateAllColumnFamilies()
        {
            cassandraCluster.RetrieveColumnFamilyConnection(RemoteLockKeyspace, RemoteLockColumnFamily).Truncate();
        }

        public const string RemoteLockKeyspace = "TestRemoteLockKeyspace";
        public const string RemoteLockColumnFamily = "TestRemoteLockCf";

        private static CassandraCluster cassandraCluster;
    }
}