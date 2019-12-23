using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;

using GroBuf;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.DistributedLock
{
    internal class CassandraBaseLockOperationsPerformer
    {
        public CassandraBaseLockOperationsPerformer(ICassandraCluster cassandraCluster, ISerializer serializer, CassandraRemoteLockImplementationSettings settings)
        {
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            timestampProvider = settings.TimestampProvider;
            keyspaceName = settings.KeyspaceName;
            columnFamilyName = settings.ColumnFamilyName;
            lockTtl = settings.LockTtl;
            lockMetadataTtl = settings.LockMetadataTtl;
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
                    exists = connection.TryGetColumn(lockRowId, columnName, out _);
                });
            return exists;
        }

        [NotNull]
        public string[] SearchThreads([NotNull] string lockRowId, long? threshold)
        {
            Column[] columns = null;
            var inclusiveStartColumnName = ThresholdToString(threshold - lockTtl.Multiply(2).Ticks);
            MakeInConnection(connection => columns = connection.GetColumns(lockRowId, inclusiveStartColumnName, endColumnName : null, count : int.MaxValue));
            return columns
                .Where(x => x.Value != null && x.Value.Length != 0)
                .Select(x => TransformColumnNameToThreadId(x.Name))
                .Distinct()
                .ToArray();
        }

        public void WriteLockMetadata([NotNull] NewLockMetadata newLockMetadata, long oldLockMetadataTimestamp)
        {
            var newTimestamp = Math.Max(GetNowTicks(), oldLockMetadataTimestamp + ticksPerMicrosecond);
            var columns = new List<Column>
                {
                    new Column
                        {
                            Name = lockRowIdColumnName,
                            Value = serializer.Serialize(newLockMetadata.LockRowId),
                            Timestamp = newTimestamp,
                            TTL = (int?)lockMetadataTtl.TotalSeconds,
                        },
                    new Column
                        {
                            Name = lockCountColumnName,
                            Value = serializer.Serialize(newLockMetadata.LockCount),
                            Timestamp = newTimestamp,
                            TTL = (int?)lockMetadataTtl.TotalSeconds,
                        },
                    new Column
                        {
                            Name = previousThresholdColumnName,
                            Value = serializer.Serialize(newLockMetadata.Threshold),
                            Timestamp = newTimestamp,
                            TTL = (int?)lockMetadataTtl.TotalSeconds,
                        },
                    new Column
                        {
                            Name = probableOwnerThreadIdColumnName,
                            Value = serializer.Serialize(newLockMetadata.OwnerThreadId),
                            Timestamp = newTimestamp,
                            TTL = (int?)lockMetadataTtl.TotalSeconds,
                        }
                };
            var rowKey = newLockMetadata.LockId.ToLockMetadataRowKey();
            MakeInConnection(connection => connection.AddBatch(rowKey, columns));
        }

        [CanBeNull]
        [SuppressMessage("ReSharper", "PossibleInvalidOperationException")]
        public LockMetadata TryGetLockMetadata([NotNull] string lockId)
        {
            Column[] columns = null;
            var rowKey = lockId.ToLockMetadataRowKey();
            MakeInConnection(connection => columns = connection.GetColumns(rowKey, allMetadataColumnNames));
            if (!columns.Any())
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
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(keyspaceName, columnFamilyName);
            action(connection);
        }

        private long GetNowTicks()
        {
            var ticks = timestampProvider.GetNowTicks();
            while (true)
            {
                var last = Interlocked.Read(ref lastTicks);
                var cur = Math.Max(ticks, last + ticksPerMicrosecond);
                if (Interlocked.CompareExchange(ref lastTicks, cur, last) == last)
                    return cur;
            }
        }

        [NotNull]
        private static string TransformThreadIdToColumnName(long? threshold, [NotNull] string threadId)
        {
            if (string.IsNullOrEmpty(threadId))
                throw new ArgumentException("Empty ThreadId is not supported", nameof(threadId));
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

        private const long ticksPerMicrosecond = 10;
        private const string threadIdWasThresholdedIndicator = "91075218575b4c14bc88ce8b00fe9946";
        private const string lockRowIdColumnName = "LockRowId";
        private const string lockCountColumnName = "LockCount";
        private const string previousThresholdColumnName = "PreviousThreshold";
        private const string probableOwnerThreadIdColumnName = "ProbableOwnerThreadId";
        private static readonly int thresholdedThreadTechnicalPrefixLength = threadIdWasThresholdedIndicator.Length + 22;
        private static readonly string[] allMetadataColumnNames = {lockCountColumnName, lockRowIdColumnName, previousThresholdColumnName, probableOwnerThreadIdColumnName};
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly ITimestampProvider timestampProvider;
        private readonly string keyspaceName;
        private readonly string columnFamilyName;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan lockMetadataTtl;
        private long lastTicks;
    }
}