using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal class CassandraBaseLockOperationsPerformer
    {
        public CassandraBaseLockOperationsPerformer(ICassandraCluster cassandraCluster, ISerializer serializer, ColumnFamilyFullName columnFamilyFullName)
        {
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            this.columnFamilyFullName = columnFamilyFullName;
        }

        public void WriteThread(string lockRowId, long? threshold, string threadId, TimeSpan ttl)
        {
            MakeInConnection(connection => connection.AddColumn(lockRowId, new Column
                {
                    Name = TransformThreadIdToColumnName(threshold, threadId),
                    Value = new byte[] {0},
                    Timestamp = GetNowTicks(),
                    TTL = (int?)ttl.TotalSeconds
                }));
        }

        public void DeleteThread(string lockRowId, long? threshold, string threadId)
        {
            MakeInConnection(connection => connection.DeleteBatch(lockRowId, new[] {TransformThreadIdToColumnName(threshold, threadId)}, GetNowTicks()));
        }

        public string[] SeatchThreads(string lockRowId, long? threshold)
        {
            var res = new string[0];
            MakeInConnection(connection =>
                {
                    var columns = connection.GetRow(lockRowId, ThresholdToString(threshold)).ToArray();
                    if(columns.Length != 0)
                        res = columns.Where(x => x.Value != null && x.Value.Length != 0).Select(x => TransformColumnNameToThreadId(x.Name)).ToArray();
                });
            return res;
        }

        public void WriteLockMetadata(string lockMetadataRowId, LockMetadata lockMetadata)
        {
            var columns = new List<Column>
                {
                    new Column
                        {
                            Name = "LockRowId",
                            Value = serializer.Serialize(lockMetadata.LockRowId),
                            Timestamp = GetNowTicks()
                        },
                    new Column
                        {
                            Name = "LockCount",
                            Value = serializer.Serialize(lockMetadata.LockCount),
                            Timestamp = GetNowTicks()
                        }
                };
            if(lockMetadata.PreviousThreshold != null)
            {
                columns.Add(new Column
                    {
                        Name = "PreviousLockOwner",
                        Value = serializer.Serialize(lockMetadata.PreviousThreshold),
                        Timestamp = GetNowTicks()
                    });
            }
            if(lockMetadata.CurrentThreshold != null)
            {
                columns.Add(new Column
                    {
                        Name = "CurrentLockOwner",
                        Value = serializer.Serialize(lockMetadata.CurrentThreshold),
                        Timestamp = GetNowTicks()
                    });
            }

            MakeInConnection(connection => connection.AddBatch(lockMetadataRowId, columns.ToArray()));
        }

        public LockMetadata GetLockMetadata(string lockMetadataRowId, string defaultLockRowId)
        {
            LockMetadata res = null;
            MakeInConnection(connection =>
                {
                    var columns = connection.GetColumns(lockMetadataRowId, new[] {"LockCount", "LockRowId", "PreviousLockOwner", "CurrentLockOwner"});
                    if(columns.Any(x => x.Name == "LockCount"))
                    {
                        res = new LockMetadata
                            {
                                LockCount = serializer.Deserialize<int>(columns.First(x => x.Name == "LockCount").Value),
                                LockRowId = columns.Any(x => x.Name == "LockRowId")
                                                ? serializer.Deserialize<string>(columns.First(x => x.Name == "LockRowId").Value)
                                                : defaultLockRowId,
                                PreviousThreshold = columns.Any(x => x.Name == "PreviousLockOwner")
                                                        ? serializer.Deserialize<long>(columns.First(x => x.Name == "PreviousLockOwner").Value)
                                                        : (long?)null,
                                CurrentThreshold = columns.Any(x => x.Name == "CurrentLockOwner")
                                                       ? serializer.Deserialize<long>(columns.First(x => x.Name == "CurrentLockOwner").Value)
                                                       : (long?)null
                            };
                    }
                });
            return res;
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            action(connection);
        }

        private long GetNowTicks()
        {
            var ticks = DateTime.UtcNow.Ticks;
            while(true)
            {
                var last = Interlocked.Read(ref lastTicks);
                var cur = Math.Max(ticks, last + 1);
                if(Interlocked.CompareExchange(ref lastTicks, cur, last) == last)
                    return cur;
            }
        }

        private static string TransformThreadIdToColumnName(long? threshold, string threadId)
        {
            if(string.IsNullOrEmpty(threadId))
                throw new ArgumentException("Empty ThreadId is not supported", "threadId");
            if(threadId.Contains(delimiterSpecialSymbol))
                throw new ArgumentException(string.Format("ThreadId cannot contains '{0}' symbol", delimiterSpecialSymbol), "threadId");
            return threshold == null ? threadId : (ThresholdToString(threshold) + delimiterSpecialSymbol + threadId);
        }

        private static string ThresholdToString(long? threshold)
        {
            return threshold == null ? null : threshold.Value.ToString("D20");
        }

        private static string TransformColumnNameToThreadId(string columnName)
        {
            return columnName.Contains(delimiterSpecialSymbol) ? columnName.Split(delimiterSpecialSymbol)[1] : columnName;
        }

        private const char delimiterSpecialSymbol = ':';
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly ColumnFamilyFullName columnFamilyFullName;
        private long lastTicks;
    }
}