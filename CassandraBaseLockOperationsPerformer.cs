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
            MakeInConnection(connection => connection.DeleteColumn(lockRowId, TransformThreadIdToColumnName(threshold, threadId), GetNowTicks()));
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
            var newTimestamp = Math.Max(GetNowTicks(), lockMetadata.PreviousPersistedTimestamp + 1 ?? 0L);
            var columns = new List<Column>
                {
                    new Column
                        {
                            Name = lockRowIdColumnName,
                            Value = serializer.Serialize(lockMetadata.LockRowId),
                            Timestamp = newTimestamp
                        },
                    new Column
                        {
                            Name = lockCountColumnName,
                            Value = serializer.Serialize(lockMetadata.LockCount),
                            Timestamp = newTimestamp
                        }
                };
            if(lockMetadata.PreviousThreshold != null)
            {
                columns.Add(new Column
                    {
                        Name = previousThresholdColumnName,
                        Value = serializer.Serialize(lockMetadata.PreviousThreshold),
                        Timestamp = newTimestamp
                    });
            }

            MakeInConnection(connection => connection.AddBatch(lockMetadata.LockId.ToLockMetadataRowKey(), columns.ToArray()));
        }

        public LockMetadata GetLockMetadata(string lockId)
        {
            LockMetadata res = null;
            MakeInConnection(connection =>
                {
                    var columns = connection.GetColumns(lockId.ToLockMetadataRowKey(), new[] {lockCountColumnName, lockRowIdColumnName, previousThresholdColumnName});
                    if(!columns.Any()) return;
                    var lockRowId = columns.Any(column => column.Name == lockRowIdColumnName) ?
                                        serializer.Deserialize<string>(columns.First(x => x.Name == lockRowIdColumnName).Value) :
                                        lockId;
                    var lockCount = columns.Any(column => column.Name == lockCountColumnName) ?
                                        serializer.Deserialize<int>(columns.First(x => x.Name == lockCountColumnName).Value) :
                                        0;
                    var previousThreshold = columns.Any(x => x.Name == previousThresholdColumnName)
                                                ? serializer.Deserialize<long>(columns.First(x => x.Name == previousThresholdColumnName).Value)
                                                : (long?)null;
                    res = new LockMetadata(lockId, lockRowId, lockCount, previousThreshold, columns.Max(column => column.Timestamp));
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
                       ThresholdToString(threshold) + ':' + threadId;
        }

        private static string ThresholdToString(long? threshold)
        {
            return threshold == null ? null : threadIdWasThresholdedIndicator + ':' + threshold.Value.ToString("D20");
        }

        private static string TransformColumnNameToThreadId(string columnName)
        {
            return columnName.StartsWith(threadIdWasThresholdedIndicator) ? columnName.Substring(thresholdedThreadTechnicalPrefixLength) : columnName;
        }

        private const string threadIdWasThresholdedIndicator = "91075218575b4c14bc88ce8b00fe9946";
        private const string lockRowIdColumnName = "LockRowId";
        private const string lockCountColumnName = "LockCount";
        private const string previousThresholdColumnName = "PreviousThreshold";
        private static readonly int thresholdedThreadTechnicalPrefixLength = threadIdWasThresholdedIndicator.Length + 22;
        
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly ColumnFamilyFullName columnFamilyFullName;
        private long lastTicks;
    }
}