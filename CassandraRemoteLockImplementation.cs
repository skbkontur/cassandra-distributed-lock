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
            var lockMetadata = baseOperationsPerformer.GetLockMetadata(lockId);
            var newThreshold = NewThreshold(lockMetadata);

            var result = RunBattle(lockMetadata, threadId, newThreshold);
            if(result.Status == LockAttemptStatus.Success)
            {
                if(lockMetadata.LockCount > changeLockRowThreshold)
                {
                    var newLockMetadata = new LockMetadata(lockId, Guid.NewGuid().ToString(), 1, newThreshold, lockMetadata.PreviousPersistedTimestamp, threadId);

                    baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, singleOperationTimeout.Multiply(3));
                    baseOperationsPerformer.WriteThread(newLockMetadata.MainRowKey(), newThreshold, threadId, singleOperationTimeout.Multiply(2) + lockTtl);
                    baseOperationsPerformer.WriteLockMetadata(newLockMetadata);
                    baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, TimeSpan.FromMinutes(20));

                    return LockAttemptResult.Success();
                }

                baseOperationsPerformer.WriteLockMetadata(new LockMetadata(lockId, lockMetadata.LockRowId, lockMetadata.LockCount + 1, newThreshold, lockMetadata.PreviousPersistedTimestamp, threadId));
            }

            return result;
        }

        private LockAttemptResult RunBattle(LockMetadata lockMetadata, string threadId, long newThreshold)
        {
            if(!string.IsNullOrEmpty(lockMetadata.ProbableOwnerThreadId) &&
               baseOperationsPerformer.ThreadAlive(lockMetadata.LockRowId, lockMetadata.PreviousThreshold, lockMetadata.ProbableOwnerThreadId))
                return LockAttemptResult.AnotherOwner(lockMetadata.ProbableOwnerThreadId);
            var items = baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold);
            if (items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if (items.Length > 1)
            {
                if (items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }

            var beforeOurWriteShades = baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.PreviousThreshold);
            if (beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            baseOperationsPerformer.WriteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId, lockTtl);
            var shades = baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.PreviousThreshold);
            if (shades.Length == 1)
            {
                items = baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold);
                if (items.Length == 0)
                {
                    baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, singleOperationTimeout + lockTtl);
                    baseOperationsPerformer.DeleteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId);
                    return LockAttemptResult.Success();
                }
            }
            baseOperationsPerformer.DeleteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public void Unlock(string lockId, string threadId)
        {
            var lockMetadata = baseOperationsPerformer.GetLockMetadata(lockId);
            baseOperationsPerformer.DeleteThread(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold, threadId);
        }

        public void Relock(string lockId, string threadId)
        {
            var lockMetadata = baseOperationsPerformer.GetLockMetadata(lockId);
            var newThreshold = NewThreshold(lockMetadata);
            var newLockMetadata = new LockMetadata(lockMetadata.LockId, lockMetadata.LockRowId, lockMetadata.LockCount, newThreshold, lockMetadata.PreviousPersistedTimestamp, threadId);
            baseOperationsPerformer.WriteThread(newLockMetadata.MainRowKey(), newLockMetadata.PreviousThreshold, threadId, lockTtl);
            baseOperationsPerformer.WriteLockMetadata(newLockMetadata);
        }

        public string[] GetLockThreads(string lockId)
        {
            var lockMetadata = baseOperationsPerformer.GetLockMetadata(lockId);
            return baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold);
        }

        public string[] GetShadeThreads(string lockId)
        {
            var lockMetadata = baseOperationsPerformer.GetLockMetadata(lockId);
            return baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.PreviousThreshold);
        }

        public long? GetThresholdValue(string lockId)
        {
            var lockMetadata = baseOperationsPerformer.GetLockMetadata(lockId);
            return lockMetadata.PreviousThreshold;
        }

        private static long NewThreshold(LockMetadata lockMetadata)
        {
            return Math.Max(DateTime.UtcNow.Ticks, lockMetadata.PreviousThreshold + 1 ?? 0);
        }

        private readonly TimeSpan singleOperationTimeout;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly int changeLockRowThreshold;
        private readonly CassandraBaseLockOperationsPerformer baseOperationsPerformer;
    }
}