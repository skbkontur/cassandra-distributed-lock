using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests.FiledCassandra
{
    public class FailedCassandraClusterException : Exception
    {
        public FailedCassandraClusterException(string message)
            : base(message)
        {
        }
    }
}