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
    internal class CassandraLockRepository
    {
        public CassandraLockRepository(ICassandraCluster cassandraCluster, ISerializer serializer, ColumnFamilyFullName columnFamilyFullName)
        {
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            this.columnFamilyFullName = columnFamilyFullName;
        }

        public string[] GetThreadsInLockRow(LockMetadata lockMetadata)
        {
            return SearchThreadsInRow(lockMetadata.PreviousLockOwner, GetMainRowKey(lockMetadata.LockRowId));
        }

        public string[] GetShadowThreadsInLockRow(LockMetadata lockMetadata)
        {
            return SearchThreadsInRow(lockMetadata.CurrentLockOwner, GetShadowRowKey(lockMetadata.LockRowId));
        }

        public void UnlockRow(LockMetadata lockMetadata, string threadId)
        {
            DeleteThreadFromRow(lockMetadata.PreviousLockOwner, GetMainRowKey(lockMetadata.LockRowId), threadId);
        }

        public void RelockRow(LockMetadata lockMetadata, string threadId, TimeSpan lockTtl)
        {
            WriteThreadToRow(lockMetadata.PreviousLockOwner, GetMainRowKey(lockMetadata.LockRowId), threadId, lockTtl);
        }

        public LockAttemptResult TryLock(LockMetadata lockMetadata, string threadId, TimeSpan lockTtl, TimeSpan ttl)
        {
            var items = GetThreadsInLockRow(lockMetadata);
            if(items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if(items.Length > 1)
            {
                if(items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }

            var beforeOurWriteShades = GetShadowThreadsInLockRow(lockMetadata);
            if(beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            WriteThreadToRow(lockMetadata.CurrentLockOwner, GetShadowRowKey(lockMetadata.LockRowId), threadId, lockTtl);
            var shades = GetShadowThreadsInLockRow(lockMetadata);
            if(shades.Length == 1)
            {
                items = GetThreadsInLockRow(lockMetadata);
                if(items.Length == 0)
                {
                    WriteThreadToRow(lockMetadata.CurrentLockOwner, GetMainRowKey(lockMetadata.LockRowId), threadId, ttl);
                    DeleteThreadFromRow(lockMetadata.CurrentLockOwner, GetShadowRowKey(lockMetadata.LockRowId), threadId);
                    return LockAttemptResult.Success();
                }
            }
            DeleteThreadFromRow(lockMetadata.CurrentLockOwner, GetShadowRowKey(lockMetadata.LockRowId), threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public void UpdateLockRowTtl(LockMetadata lockMetadata, string threadId, TimeSpan ttl)
        {
            WriteThreadToRow(lockMetadata.CurrentLockOwner, GetMainRowKey(lockMetadata.LockRowId), threadId, ttl);
        }

        public void LockRowUnSafe(LockMetadata lockMetadata, string threadId, TimeSpan ttl)
        {
            WriteThreadToRow(lockMetadata.PreviousLockOwner, GetMainRowKey(lockMetadata.LockRowId), threadId, ttl);
        }
        
        public void WriteLockMetadata(string lockId, LockMetadata lockMetadata)
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
            if(lockMetadata.PreviousLockOwner != null)
            {
                columns.Add(new Column
                    {
                        Name = "PreviousLockOwner",
                        Value = serializer.Serialize(lockMetadata.PreviousLockOwner),
                        Timestamp = GetNowTicks()
                    });
            }
            if(lockMetadata.CurrentLockOwner != null)
            {
                columns.Add(new Column
                    {
                        Name = "CurrentLockOwner",
                        Value = serializer.Serialize(lockMetadata.CurrentLockOwner),
                        Timestamp = GetNowTicks()
                    });
            }

            MakeInConnection(connection => connection.AddBatch(GetLockMetadataRowKey(lockId), columns.ToArray()));
        }

        public LockMetadata GetLockMetadata(string lockId, string defaultLockRowId)
        {
            var row = GetLockMetadataRowKey(lockId);
            LockMetadata res = null;
            MakeInConnection(connection =>
                {
                    var columns = connection.GetColumns(row, new[] { "LockCount", "LockRowId", "PreviousLockOwner", "CurrentLockOwner" });
                    if(columns.Any(x => x.Name == "LockCount"))
                    {
                        res = new LockMetadata
                            {
                                LockCount = serializer.Deserialize<int>(columns.First(x => x.Name == "LockCount").Value),
                                LockRowId = columns.Any(x => x.Name == "LockRowId")
                                                ? serializer.Deserialize<string>(columns.First(x => x.Name == "LockRowId").Value)
                                                : defaultLockRowId,
                                PreviousLockOwner = columns.Any(x => x.Name == "PreviousLockOwner")
                                                ? serializer.Deserialize<LockOwner>(columns.First(x => x.Name == "PreviousLockOwner").Value)
                                                : null,
                                CurrentLockOwner = columns.Any(x => x.Name == "CurrentLockOwner")
                                                ? serializer.Deserialize<LockOwner>(columns.First(x => x.Name == "CurrentLockOwner").Value)
                                                : null
                            };
                    }
                });
            return res;
        }

        public void IncrementLockCount(string lockId, LockMetadata lockMetadata)
        {
            lockMetadata.LockCount++;

            MakeInConnection(connection => connection.AddColumn(
                GetLockMetadataRowKey(lockId),
                new Column
                {
                    Name = "LockCount",
                    Value = serializer.Serialize(lockMetadata.LockCount),
                    Timestamp = GetNowTicks()
                }
                                               ));
        }

        private static string GetShadowRowKey(string lockId)
        {
            return "Shade_" + lockId;
        }

        private static string GetMainRowKey(string lockId)
        {
            return "Main_" + lockId;
        }

        private static string GetLockMetadataRowKey(string lockId)
        {
            return "Metadata_" + lockId;
        }

        private void WriteThreadToRow(LockOwner lockOwner, string rowName, string threadId, TimeSpan ttl)
        {
            MakeInConnection(connection => connection.AddColumn(rowName, new Column
                {
                    Name = TransformThreadIdToColumnName(lockOwner, threadId),
                    Value = new byte[] {0},
                    Timestamp = GetNowTicks(),
                    TTL = (int?)ttl.TotalSeconds
                }));
        }

        private string[] SearchThreadsInRow(LockOwner lockOwner, string rowName)
        {
            var res = new string[0];
            MakeInConnection(connection =>
                {
                    var columns = connection.GetRow(rowName, GetLockRowOperationsThreshold(lockOwner)).ToArray();
                    if(columns.Length != 0)
                        res = columns.Where(x => x.Value != null && x.Value.Length != 0).Select(x => TransformColumnNameToThreadId(x.Name)).ToArray();
                });
            return res;
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

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            action(connection);
        }

        private void DeleteThreadFromRow(LockOwner lockOwner, string rowName, string threadId)
        {
            MakeInConnection(connection => connection.DeleteBatch(rowName, new[] { TransformThreadIdToColumnName(lockOwner, threadId) }, GetNowTicks()));
        }

        private string TransformThreadIdToColumnName(LockOwner lockOwner, string threadId)
        {
            if (string.IsNullOrEmpty(threadId))
                throw new ArgumentException("Empty ThreadId is not supported", "threadId");
            if (threadId.Contains(delimiterSpecialSymbol))
                throw new ArgumentException(string.Format("ThreadId cannot contains '{0}' symbol", delimiterSpecialSymbol), "threadId");
            var threshold = GetLockRowOperationsThreshold(lockOwner);
            return threshold == null ? threadId : threshold + delimiterSpecialSymbol + threadId;
       }

        private string GetLockRowOperationsThreshold(LockOwner lockOwner)
        {
            return lockOwner == null ? null : lockOwner.LockRowThreshold.ToString();
        }

        private string TransformColumnNameToThreadId(string columnName)
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