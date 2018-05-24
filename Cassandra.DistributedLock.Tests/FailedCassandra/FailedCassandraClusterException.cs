using System;

namespace Cassandra.DistributedLock.Tests.FailedCassandra
{
    public class FailedCassandraClusterException : Exception
    {
        public FailedCassandraClusterException(string message)
            : base(message)
        {
        }
    }
}