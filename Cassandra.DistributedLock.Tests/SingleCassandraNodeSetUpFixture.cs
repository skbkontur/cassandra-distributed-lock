using System;
using System.IO;
using System.Net;

using Cassandra.DistributedLock.Tests.Logging;

using log4net;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Scheme;

using Vostok.Logging.Log4net;

namespace Cassandra.DistributedLock.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        [OneTimeSetUp]
        public static void SetUp()
        {
            Log4NetConfiguration.InitializeOnce();
            var templateDirectory = Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"v3.11.x");
            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra_v3.11.x");
            node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400,
                };
            node.Restart();

            var logger = new Log4netLog(LogManager.GetLogger(typeof(SingleCassandraNodeSetUpFixture)));
            cassandraCluster = new CassandraCluster(CreateCassandraClusterSettings(), logger);
            cassandraCluster.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
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
                });
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            node.Stop();
        }

        public static ICassandraClusterSettings CreateCassandraClusterSettings(int attempts = 5, TimeSpan? timeout = null)
        {
            return new SingleNodeCassandraClusterSettings(new IPEndPoint(IPAddress.Parse(node.RpcAddress), node.RpcPort))
                {
                    ClusterName = node.ClusterName,
                    Attempts = attempts,
                    Timeout = (int)(timeout ?? TimeSpan.FromSeconds(6)).TotalMilliseconds,
                };
        }

        public static void TruncateAllColumnFamilies()
        {
            cassandraCluster.RetrieveColumnFamilyConnection(RemoteLockKeyspace, RemoteLockColumnFamily).Truncate();
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if(currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        public const string RemoteLockKeyspace = "TestRemoteLockKeyspace";
        public const string RemoteLockColumnFamily = "TestRemoteLockCf";

        private const string cassandraTemplates = @"cassandra-local\cassandra";

        private static LocalCassandraNode node;
        private static CassandraCluster cassandraCluster;
    }
}