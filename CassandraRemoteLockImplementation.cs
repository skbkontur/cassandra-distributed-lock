using System;
using System.Linq;

using GroBuf;

using JetBrains.Annotations;

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

        [NotNull]
        public LockAttemptResult TryLock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = baseOperationsPerformer.TryGetLockMetadata(lockId) ?? new LockMetadata(lockId, lockId, 0, null, null, 0L);
            var newThreshold = NewThreshold(lockMetadata.PreviousThreshold ?? (DateTime.UtcNow - TimeSpan.FromHours(1)).Ticks);

            LockAttemptResult result;
            var probableOwnerThreadId = lockMetadata.ProbableOwnerThreadId;
            if(!string.IsNullOrEmpty(probableOwnerThreadId) && baseOperationsPerformer.ThreadAlive(lockMetadata.LockRowId, lockMetadata.PreviousThreshold, probableOwnerThreadId))
                result = probableOwnerThreadId == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(probableOwnerThreadId);
            else
                result = RunBattle(lockMetadata, threadId, newThreshold);

            if(result.Status == LockAttemptStatus.Success)
            {
                lockMetadata = baseOperationsPerformer.TryGetLockMetadata(lockId) ?? new LockMetadata(lockId, lockId, 0, null, null, 0L);

                if(lockMetadata.LockCount <= changeLockRowThreshold)
                {
                    var newLockMetadata = new NewLockMetadata(lockId, lockMetadata.LockRowId, lockMetadata.LockCount + 1, newThreshold, threadId);
                    baseOperationsPerformer.WriteLockMetadata(newLockMetadata, lockMetadata.Timestamp);
                }
                else
                {
                    var newLockMetadata = new NewLockMetadata(lockId, Guid.NewGuid().ToString(), 1, newThreshold, threadId);
                    baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, singleOperationTimeout.Multiply(3));
                    baseOperationsPerformer.WriteThread(newLockMetadata.MainRowKey(), newThreshold, threadId, singleOperationTimeout.Multiply(2) + lockTtl);
                    baseOperationsPerformer.WriteLockMetadata(newLockMetadata, lockMetadata.Timestamp);
                    baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, TimeSpan.FromMinutes(20));
                }
            }
            return result;
        }

        [NotNull]
        private LockAttemptResult RunBattle([NotNull] LockMetadata lockMetadata, [NotNull] string threadId, long newThreshold)
        {
            var items = baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold);
            if(items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if(items.Length > 1)
            {
                if(items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }
            var beforeOurWriteShades = baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.PreviousThreshold);
            if(beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            baseOperationsPerformer.WriteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId, lockTtl);
            var shades = baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.PreviousThreshold);
            if(shades.Length == 1)
            {
                items = baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold);
                if(items.Length == 0)
                {
                    baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, singleOperationTimeout + lockTtl);
                    baseOperationsPerformer.DeleteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId);
                    return LockAttemptResult.Success();
                }
            }
            baseOperationsPerformer.DeleteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public bool TryUnlock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = baseOperationsPerformer.TryGetLockMetadata(lockId);
            if(lockMetadata == null)
                return false;
            baseOperationsPerformer.DeleteThread(lockMetadata.MainRowKey(), lockMetadata.GetPreviousThreshold(), threadId);
            return true;
        }

        public bool TryRelock([NotNull] string lockId, [NotNull] string threadId)
        {
            var lockMetadata = baseOperationsPerformer.TryGetLockMetadata(lockId);
            if(lockMetadata == null)
                return false;
            var newThreshold = NewThreshold(lockMetadata.GetPreviousThreshold());
            var newLockMetadata = new NewLockMetadata(lockMetadata.LockId, lockMetadata.LockRowId, lockMetadata.LockCount, newThreshold, threadId);
            baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, lockTtl);
            baseOperationsPerformer.WriteLockMetadata(newLockMetadata, lockMetadata.Timestamp);
            return true;
        }

        [NotNull]
        public string[] GetLockThreads([NotNull] string lockId)
        {
            var lockMetadata = GetLockMetadata(lockId);
            return baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.GetPreviousThreshold());
        }

        [NotNull]
        public string[] GetShadeThreads([NotNull] string lockId)
        {
            var lockMetadata = GetLockMetadata(lockId);
            return baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.GetPreviousThreshold());
        }

        [NotNull]
        public LockMetadata GetLockMetadata([NotNull] string lockId)
        {
            var lockMetadata = baseOperationsPerformer.TryGetLockMetadata(lockId);
            if(lockMetadata == null)
                throw new InvalidOperationException(string.Format("Not found metadata for lockId = {0}", lockId));
            return lockMetadata;
        }

        private static long NewThreshold(long previousThreshold)
        {
            return Math.Max(DateTime.UtcNow.Ticks, previousThreshold + 1);
        }

        private readonly TimeSpan singleOperationTimeout;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly int changeLockRowThreshold;
        private readonly CassandraBaseLockOperationsPerformer baseOperationsPerformer;
    }
}