using System;
using System.Linq;
using System.Threading;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal class CassandraLockRepository
    {
        public CassandraLockRepository(ICassandraCluster cassandraCluster, ISerializer serializer, TimeSpan lockTTL, string keyspace, string columnFamily)
        {
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            this.lockTTL = lockTTL;
            this.keyspace = keyspace;
            this.columnFamily = columnFamily;
        }

        public string[] GetThreadsInLockRow(string lockRowId)
        {
            return Search(GetMainRowKey(lockRowId));
        }

        public string[] GetShadowThreadsInLockRow(string lockRowId)
        {
            return Search(GetShadowRowKey(lockRowId));
        }

        public void UnlockRow(string lockRowId, string threadId)
        {
            Delete(GetMainRowKey(lockRowId), threadId);
        }

        public void RelockRow(string lockRowId, string threadId)
        {
            WriteLockRow(GetMainRowKey(lockRowId), threadId);
        }

        public LockAttemptResult TryLock(string lockId, string threadId, TimeSpan ttl)
        {
            var items = GetThreadsInLockRow(lockId);
            if(items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if(items.Length > 1)
            {
                if(items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }

            var beforeOurWriteShades = GetShadowThreadsInLockRow(lockId);
            if(beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            WriteLockRow(GetShadowRowKey(lockId), threadId);
            var shades = GetShadowThreadsInLockRow(lockId);
            if(shades.Length == 1)
            {
                items = GetThreadsInLockRow(lockId);
                if(items.Length == 0)
                {
                    WriteLockRow(GetMainRowKey(lockId), threadId, ttl);
                    Delete(GetShadowRowKey(lockId), threadId);
                    return LockAttemptResult.Success();
                }
            }
            Delete(GetShadowRowKey(lockId), threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public void UpdateLockRowTTL(string lockRowId, string threadId, TimeSpan ttl)
        {
            WriteLockRow(GetMainRowKey(lockRowId), threadId, ttl);
        }

        public void LockRowUnSafe(string lockRowId, string threadId, TimeSpan ttl)
        {
            WriteLockRow(GetMainRowKey(lockRowId), threadId, ttl);
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
                    var columns = connection.GetRow(row).ToArray();
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

        private string GetShadowRowKey(string lockId)
        {
            return "Shade_" + lockId;
        }

        private string GetMainRowKey(string lockId)
        {
            return "Main_" + lockId;
        }

        private string GetLockMetadataRowKey(string lockId)
        {
            return "Metadata_" + lockId;
        }

        private void WriteLockRow(string rowName, string threadId, TimeSpan? ttl = null)
        {
            MakeInConnection(connection => connection.AddColumn(rowName, new Column
                {
                    Name = threadId,
                    Value = new byte[] {0},
                    Timestamp = GetNowTicks(),
                    TTL = (int?)ttl.GetValueOrDefault(lockTTL).TotalSeconds
                }));
        }

        private string[] Search(string rowName)
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
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(keyspace, columnFamily);
            action(connection);
        }

        private void Delete(string rowName, string threadId)
        {
            MakeInConnection(connection => connection.DeleteBatch(rowName, new[] {threadId}, GetNowTicks()));
        }

        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly TimeSpan lockTTL;
        private readonly string keyspace;
        private readonly string columnFamily;

        private long lastTicks;
    }
}