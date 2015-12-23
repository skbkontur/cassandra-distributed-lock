using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using GroBuf;

using JetBrains.Annotations;

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

        public void WriteThread([NotNull] string lockRowId, long threshold, [NotNull] string threadId, TimeSpan ttl)
        {
            var column = new Column
                {
                    Name = TransformThreadIdToColumnName(threshold, threadId),
                    Value = new byte[] {0},
                    Timestamp = GetNowTicks(),
                    TTL = (int?)ttl.TotalSeconds,
                };
            MakeInConnection(connection => connection.AddColumn(lockRowId, column));
        }

        public void DeleteThread([NotNull] string lockRowId, long threshold, [NotNull] string threadId)
        {
            var columnName = TransformThreadIdToColumnName(threshold, threadId);
            var timestamp = GetNowTicks();
            MakeInConnection(connection => connection.DeleteColumn(lockRowId, columnName, timestamp));
        }

        public bool ThreadAlive([NotNull] string lockRowId, long? threshold, [NotNull] string threadId)
        {
            var exists = false;
            var columnName = TransformThreadIdToColumnName(threshold, threadId);
            MakeInConnection(connection =>
                {
                    Column threadColumn;
                    exists = connection.TryGetColumn(lockRowId, columnName, out threadColumn);
                });
            return exists;
        }

        [NotNull]
        public string[] SearchThreads([NotNull] string lockRowId, long? threshold)
        {
            Column[] columns = null;
            var exclusiveStartColumnName = ThresholdToString(threshold - TimeSpan.FromMinutes(5).Ticks);
            MakeInConnection(connection => columns = connection.GetRow(lockRowId, exclusiveStartColumnName).ToArray());
            return columns
                .Where(x => x.Value != null && x.Value.Length != 0)
                .Select(x => TransformColumnNameToThreadId(x.Name))
                .Distinct()
                .ToArray();
        }

        public void WriteLockMetadata([NotNull] NewLockMetadata newLockMetadata, long oldLockMetadataTimestamp)
        {
            var newTimestamp = Math.Max(GetNowTicks(), oldLockMetadataTimestamp + 1);
            var columns = new List<Column>
                {
                    new Column
                        {
                            Name = lockRowIdColumnName,
                            Value = serializer.Serialize(newLockMetadata.LockRowId),
                            Timestamp = newTimestamp,
                            TTL = null,
                        },
                    new Column
                        {
                            Name = lockCountColumnName,
                            Value = serializer.Serialize(newLockMetadata.LockCount),
                            Timestamp = newTimestamp,
                            TTL = null,
                        },
                    new Column
                        {
                            Name = previousThresholdColumnName,
                            Value = serializer.Serialize(newLockMetadata.Threshold),
                            Timestamp = newTimestamp,
                            TTL = null,
                        },
                    new Column
                        {
                            Name = probableOwnerThreadIdColumnName,
                            Value = serializer.Serialize(newLockMetadata.OwnerThreadId),
                            Timestamp = newTimestamp,
                            TTL = null,
                        }
                };
            var rowKey = newLockMetadata.LockId.ToLockMetadataRowKey();
            MakeInConnection(connection => connection.AddBatch(rowKey, columns));
        }

        [CanBeNull]
        public LockMetadata TryGetLockMetadata([NotNull] string lockId)
        {
            Column[] columns = null;
            var rowKey = lockId.ToLockMetadataRowKey();
            MakeInConnection(connection => columns = connection.GetColumns(rowKey, allMetadataColumnNames));
            if(!columns.Any())
                return null;
            var lockRowIdColumn = columns.SingleOrDefault(x => x.Name == lockRowIdColumnName);
            var lockRowId = lockRowIdColumn == null ? lockId : serializer.Deserialize<string>(lockRowIdColumn.Value);
            var lockCountColumn = columns.SingleOrDefault(x => x.Name == lockCountColumnName);
            var lockCount = lockCountColumn == null ? 0 : serializer.Deserialize<int>(lockCountColumn.Value);
            var previousThresholdColumn = columns.SingleOrDefault(x => x.Name == previousThresholdColumnName);
            var previousThreshold = previousThresholdColumn == null ? (long?)null : serializer.Deserialize<long>(previousThresholdColumn.Value);
            var probableOwnerThreadIdColumn = columns.SingleOrDefault(x => x.Name == probableOwnerThreadIdColumnName);
            var probableOwnerThreadId = probableOwnerThreadIdColumn == null ? null : serializer.Deserialize<string>(probableOwnerThreadIdColumn.Value);
            var timestamp = columns.Max(column => column.Timestamp.Value);
            return new LockMetadata(lockId, lockRowId, lockCount, previousThreshold, probableOwnerThreadId, timestamp);
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            action(connection);
        }

        private readonly object lockObject = new object();

        private long GetNowTicks()
        {
            lock(lockObject)
            {
                var result = Math.Max(DateTime.UtcNow.Ticks, lastTicks + 1);
                lastTicks = result;
                return result;
            }
            /*var ticks = DateTime.UtcNow.Ticks;
            while(true)
            {
                var last = Interlocked.Read(ref lastTicks);
                var cur = Math.Max(ticks, last + 1);
                if(Interlocked.CompareExchange(ref lastTicks, cur, last) == last)
                    return cur;
            }*/
        }

        [NotNull]
        private static string TransformThreadIdToColumnName(long? threshold, [NotNull] string threadId)
        {
            if(string.IsNullOrEmpty(threadId))
                throw new ArgumentException("Empty ThreadId is not supported", "threadId");
            return threshold == null ?
                       threadId :
                       ThresholdToString(threshold) + ':' + threadId;
        }

        [CanBeNull]
        private static string ThresholdToString(long? threshold)
        {
            return threshold == null ? null : threadIdWasThresholdedIndicator + ':' + threshold.Value.ToString("D20");
        }

        [NotNull]
        private static string TransformColumnNameToThreadId([NotNull] string columnName)
        {
            return columnName.StartsWith(threadIdWasThresholdedIndicator) ? columnName.Substring(thresholdedThreadTechnicalPrefixLength) : columnName;
        }

        private const string threadIdWasThresholdedIndicator = "91075218575b4c14bc88ce8b00fe9946";
        private const string lockRowIdColumnName = "LockRowId";
        private const string lockCountColumnName = "LockCount";
        private const string previousThresholdColumnName = "PreviousThreshold";
        private const string probableOwnerThreadIdColumnName = "ProbableOwnerThreadId";
        private static readonly int thresholdedThreadTechnicalPrefixLength = threadIdWasThresholdedIndicator.Length + 22;
        private static readonly string[] allMetadataColumnNames = {lockCountColumnName, lockRowIdColumnName, previousThresholdColumnName, probableOwnerThreadIdColumnName};
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly ColumnFamilyFullName columnFamilyFullName;
        private long lastTicks;
    }
}