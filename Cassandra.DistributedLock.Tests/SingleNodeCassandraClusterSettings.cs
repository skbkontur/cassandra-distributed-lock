using System;
using System.Net;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;

namespace Cassandra.DistributedLock.Tests
{
    public class SingleNodeCassandraClusterSettings : ICassandraClusterSettings
    {
        public SingleNodeCassandraClusterSettings(IPEndPoint thriftEndpoint)
        {
            Endpoints = new[] {thriftEndpoint};
            EndpointForFierceCommands = thriftEndpoint;
        }

        public string ClusterName { get; set; }
        public ConsistencyLevel ReadConsistencyLevel => ConsistencyLevel.QUORUM;
        public ConsistencyLevel WriteConsistencyLevel => ConsistencyLevel.QUORUM;
        public IPEndPoint[] Endpoints { get; }
        public IPEndPoint EndpointForFierceCommands { get; }
        public bool AllowNullTimestamp => false;
        public int Attempts { get; set; }
        public int Timeout { get; set; }
        public int FierceTimeout => (int)TimeSpan.FromSeconds(30.0).TotalMilliseconds;
        public TimeSpan? ConnectionIdleTimeout => TimeSpan.FromSeconds(30);
        public bool EnableMetrics => false;
    }
}