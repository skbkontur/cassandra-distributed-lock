using System;
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
            return SearchThreadsInRow(GetMainRowKey(lockMetadata.LockRowId));
        }

        public string[] GetShadowThreadsInLockRow(LockMetadata lockMetadata)
        {
            return SearchThreadsInRow(GetShadowRowKey(lockMetadata.LockRowId));
        }

        public void UnlockRow(LockMetadata lockMetadata, string threadId)
        {
            DeleteThreadFromRow(GetMainRowKey(lockMetadata.LockRowId), threadId);
        }

        public void RelockRow(LockMetadata lockMetadata, string threadId, TimeSpan lockTtl)
        {
            WriteThreadToRow(GetMainRowKey(lockMetadata.LockRowId), threadId, lockTtl);
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
            WriteThreadToRow(GetShadowRowKey(lockMetadata.LockRowId), threadId, lockTtl);
            var shades = GetShadowThreadsInLockRow(lockMetadata);
            if(shades.Length == 1)
            {
                items = GetThreadsInLockRow(lockMetadata);
                if(items.Length == 0)
                {
                    WriteThreadToRow(GetMainRowKey(lockMetadata.LockRowId), threadId, ttl);
                    DeleteThreadFromRow(GetShadowRowKey(lockMetadata.LockRowId), threadId);
                    return LockAttemptResult.Success();
                }
            }
            DeleteThreadFromRow(GetShadowRowKey(lockMetadata.LockRowId), threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public void UpdateLockRowTtl(LockMetadata lockMetadata, string threadId, TimeSpan ttl)
        {
            WriteThreadToRow(GetMainRowKey(lockMetadata.LockRowId), threadId, ttl);
        }

        public void LockRowUnSafe(LockMetadata lockMetadata, string threadId, TimeSpan ttl)
        {
            WriteThreadToRow(GetMainRowKey(lockMetadata.LockRowId), threadId, ttl);
        }

        public void WriteLockMetadata(string lockId, LockMetadata lockMetadata)
        {
            MakeInConnection(connection => connection.AddBatch(
                GetLockMetadataRowKey(lockId),
                new[]
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
                    }
                                               ));
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

        public LockMetadata GetLockMetadata(string lockId, string defaultLockRowId)
        {
            var row = GetLockMetadataRowKey(lockId);
            LockMetadata res = null;
            MakeInConnection(connection =>
                {
                    var columns = connection.GetColumns(row, new []{"LockCount", "LockRowId"});
                    if(columns.Any(x => x.Name == "LockCount"))
                    {
                        res = new LockMetadata
                            {
                                LockCount = serializer.Deserialize<int>(columns.First(x => x.Name == "LockCount").Value),
                                LockRowId = columns.Any(x => x.Name == "LockRowId")
                                                ? serializer.Deserialize<string>(columns.First(x => x.Name == "LockRowId").Value)
                                                : defaultLockRowId,
                            };
                    }
                });
            return res;
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

        private void WriteThreadToRow(string rowName, string threadId, TimeSpan ttl)
        {
            MakeInConnection(connection => connection.AddColumn(rowName, new Column
                {
                    Name = threadId,
                    Value = new byte[] {0},
                    Timestamp = GetNowTicks(),
                    TTL = (int?)ttl.TotalSeconds
                }));
        }

        private string[] SearchThreadsInRow(string rowName)
        {
            var res = new string[0];
            MakeInConnection(connection =>
                {
                    var columns = connection.GetRow(rowName).ToArray();
                    if(columns.Length != 0)
                        res = columns.Where(x => x.Value != null && x.Value.Length != 0).Select(x => x.Name).ToArray();
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

        private void DeleteThreadFromRow(string rowName, string threadId)
        {
            MakeInConnection(connection => connection.DeleteBatch(rowName, new[] {threadId}, GetNowTicks()));
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly ColumnFamilyFullName columnFamilyFullName;
        private long lastTicks;
    }
}