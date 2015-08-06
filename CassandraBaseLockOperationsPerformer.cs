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

        public string[] SearchThreads(string lockRowId, long? threshold)
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

        public void WriteLockMetadata(LockMetadata lockMetadata)
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

            MakeInConnection(connection => connection.AddBatch(lockMetadata.LockId.ToLockMetadataRowKey(), columns.ToArray()));
        }

        public LockMetadata GetLockMetadata(string lockId)
        {
            LockMetadata res = null;
            MakeInConnection(connection =>
                {
                    var columns = connection.GetColumns(lockId.ToLockMetadataRowKey(), new[] {"LockCount", "LockRowId", "PreviousLockOwner", "CurrentLockOwner"});
                    if(!columns.Any()) return;
                    var lockRowId = serializer.Deserialize<string>(columns.First(x => x.Name == "LockRowId").Value);
                    var lockCount = serializer.Deserialize<int>(columns.First(x => x.Name == "LockCount").Value);
                    var previousThreshold = columns.Any(x => x.Name == "PreviousLockOwner")
                                                ? serializer.Deserialize<long>(columns.First(x => x.Name == "PreviousLockOwner").Value)
                                                : (long?)null;
                    var currentThreshold = columns.Any(x => x.Name == "CurrentLockOwner")
                                               ? serializer.Deserialize<long>(columns.First(x => x.Name == "CurrentLockOwner").Value)
                                               : (long?)null;
                    res = new LockMetadata(lockId, lockRowId, lockCount, previousThreshold, currentThreshold);
                });
            return res ?? new LockMetadata(lockId, lockId, 0, null, null);
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
            return threshold == null ?
                       threadId :
                       threadIdWasThresholdedIndicator + ':' + ThresholdToString(threshold) + ':' + threadId;
        }

        private static string ThresholdToString(long? threshold)
        {
            return threshold == null ? null : threshold.Value.ToString("D20");
        }

        private static string TransformColumnNameToThreadId(string columnName)
        {
            return columnName.StartsWith(threadIdWasThresholdedIndicator) ? columnName.Substring(thresholdedThreadTechnicalPrefixLength) : columnName;
        }

        private const string threadIdWasThresholdedIndicator = "91075218575b4c14bc88ce8b00fe9946";
        private static readonly int thresholdedThreadTechnicalPrefixLength = threadIdWasThresholdedIndicator.Length + 22;
        
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly ColumnFamilyFullName columnFamilyFullName;
        private long lastTicks;
    }
}