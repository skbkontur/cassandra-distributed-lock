using System;
using System.Linq;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class CassandraRemoteLockImplementation : IRemoteLockImplementation
    {
        public CassandraRemoteLockImplementation(ICassandraCluster cassandraCluster, ISerializer serializer, CassandraRemoteLockImplementationSettings settings)
        {
            var connectionParameters = cassandraCluster.RetrieveColumnFamilyConnection(settings.ColumnFamilyFullName.KeyspaceName, settings.ColumnFamilyFullName.ColumnFamilyName).GetConnectionParameters();
            singleOperationTimeout = TimeSpan.FromMilliseconds(connectionParameters.Attempts * connectionParameters.Timeout);
            lockTtl = settings.LockTtl;
            keepLockAliveInterval = settings.KeepLockAliveInterval;
            baseOperationsPerformer = new CassandraBaseLockOperationsPerformer(cassandraCluster, serializer, settings.ColumnFamilyFullName);
            changeLockRowThreshold = settings.ChangeLockRowThreshold;
        }

        public TimeSpan KeepLockAliveInterval { get { return keepLockAliveInterval; } }

        public LockAttemptResult TryLock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);

            var result = RunBattle(lockMetadata, threadId, singleOperationTimeout + lockTtl);
            if(result.Status == LockAttemptStatus.Success)
            {
                var newLockMetadata = NewLockMetadata(lockMetadata, threadId);

                if(lockMetadata.LockCount > changeLockRowThreshold)
                {
                    newLockMetadata.LockCount = 1;
                    newLockMetadata.LockRowId = Guid.NewGuid().ToString();

                    baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.CurrentThreshold, threadId, singleOperationTimeout.Multiply(3));
                    baseOperationsPerformer.WriteThread(newLockMetadata.LockRowId.ToMainRowKey(), newLockMetadata.PreviousThreshold, threadId, singleOperationTimeout.Multiply(2) + lockTtl);
                    baseOperationsPerformer.WriteLockMetadata(lockId.ToLockMetadataRowKey(), newLockMetadata);
                    baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.CurrentThreshold, threadId, TimeSpan.FromDays(7));

                    return LockAttemptResult.Success();
                }

                newLockMetadata.LockCount = lockMetadata.LockCount + 1;
                newLockMetadata.LockRowId = lockMetadata.LockRowId;
                baseOperationsPerformer.WriteLockMetadata(lockId.ToLockMetadataRowKey(), newLockMetadata);
            }

            return result;
        }

        private LockAttemptResult RunBattle(LockMetadata lockMetadata, string threadId, TimeSpan ttl)
        {
            var items = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold);
            if (items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if (items.Length > 1)
            {
                if (items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }

            var beforeOurWriteShades = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold);
            if (beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold, threadId, lockTtl);
            var shades = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold);
            if (shades.Length == 1)
            {
                items = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold);
                if (items.Length == 0)
                {
                    baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.CurrentThreshold, threadId, ttl);
                    baseOperationsPerformer.DeleteThread(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold, threadId);
                    return LockAttemptResult.Success();
                }
            }
            baseOperationsPerformer.DeleteThread(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold, threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public void Unlock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            baseOperationsPerformer.DeleteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold, threadId);
        }

        public void Relock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold, threadId, lockTtl);
        }

        public string[] GetLockThreads(string lockId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            return baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold);
        }

        public string[] GetShadeThreads(string lockId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            return baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold);
        }

        private LockMetadata GetOrCreateLockMetadata(string lockId)
        {
            var result = baseOperationsPerformer.GetLockMetadata(lockId.ToLockMetadataRowKey(), lockId);
            return result ?? new LockMetadata {LockCount = 0, LockRowId = lockId};
        }

        private LockMetadata NewLockMetadata(LockMetadata currentLockMetadata, string threadId)
        {
            return new LockMetadata
                {
                    PreviousThreshold = currentLockMetadata.CurrentThreshold,
                    CurrentThreshold = Math.Max(DateTime.UtcNow.Ticks, currentLockMetadata.CurrentThreshold ?? 0)
                };
        }

        private readonly TimeSpan singleOperationTimeout;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly int changeLockRowThreshold;
        private readonly CassandraBaseLockOperationsPerformer baseOperationsPerformer;
    }
}