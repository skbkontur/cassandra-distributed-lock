using System;
using System.Linq;

using GroBuf;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class CassandraRemoteLockImplementation : IRemoteLockImplementation
    {
        public CassandraRemoteLockImplementation(ICassandraCluster cassandraCluster, ISerializer serializer, CassandraRemoteLockImplementationSettings settings)
        {
            metrics = new RemoteLockerMetricsBenchTmp();
            var connectionParameters = cassandraCluster.RetrieveColumnFamilyConnection(settings.ColumnFamilyFullName.KeyspaceName, settings.ColumnFamilyFullName.ColumnFamilyName).GetConnectionParameters();
            singleOperationTimeout = TimeSpan.FromMilliseconds(connectionParameters.Attempts * connectionParameters.Timeout);
            lockTtl = settings.LockTtl;
            keepLockAliveInterval = settings.KeepLockAliveInterval;
            changeLockRowThreshold = settings.ChangeLockRowThreshold;
            timestampProvider = settings.TimestampProvider;
            baseOperationsPerformer = new CassandraBaseLockOperationsPerformer(cassandraCluster, serializer, settings);
        }

        public TimeSpan KeepLockAliveInterval { get { return keepLockAliveInterval; } }

        [NotNull]
        public LockAttemptResult TryLock([NotNull] string lockId, [NotNull] string threadId)
        {
            LockMetadata lockMetadata;
            using(metrics.TryGetLockMetadata1.NewContext(FormatLockOperationId(lockId, threadId)))
                lockMetadata = baseOperationsPerformer.TryGetLockMetadata(lockId) ?? new LockMetadata(lockId, lockId, 0, null, null, 0L);
            var newThreshold = NewThreshold(lockMetadata.PreviousThreshold ?? (DateTime.UtcNow - TimeSpan.FromHours(1)).Ticks);

            LockAttemptResult result;
            var probableOwnerThreadId = lockMetadata.ProbableOwnerThreadId;

            bool condition;
            using(metrics.TryGetLockMetadata1.NewContext(FormatLockOperationId(lockId, threadId)))
                condition = !string.IsNullOrEmpty(probableOwnerThreadId) && baseOperationsPerformer.ThreadAlive(lockMetadata.LockRowId, lockMetadata.PreviousThreshold, probableOwnerThreadId);

            if(condition)
            {
                if(probableOwnerThreadId == threadId)
                    throw new InvalidOperationException(string.Format("TryLock(lockId = {0}, threadId = {1}): probableOwnerThreadId == threadId, though it seemed to be impossible!", lockId, threadId));
                result = LockAttemptResult.AnotherOwner(probableOwnerThreadId);
            }
            else
            {
                using (metrics.RunBattle.NewContext(FormatLockOperationId(lockId, threadId)))
                    result = RunBattle(lockMetadata, threadId, newThreshold);
            }

            if(result.Status == LockAttemptStatus.Success)
            {
                using (metrics.TryGetLockMetadata2.NewContext(FormatLockOperationId(lockId, threadId)))
                    lockMetadata = baseOperationsPerformer.TryGetLockMetadata(lockId) ?? new LockMetadata(lockId, lockId, 0, null, null, 0L);

                if(lockMetadata.LockCount <= changeLockRowThreshold)
                {
                    var newLockMetadata = new NewLockMetadata(lockId, lockMetadata.LockRowId, lockMetadata.LockCount + 1, newThreshold, threadId);
                    using (metrics.WriteLockMetadata1.NewContext(FormatLockOperationId(lockId, threadId)))
                        baseOperationsPerformer.WriteLockMetadata(newLockMetadata, lockMetadata.Timestamp);
                }
                else
                {
                    var newLockMetadata = new NewLockMetadata(lockId, Guid.NewGuid().ToString(), 1, newThreshold, threadId);
                    using (metrics.WriteThread1.NewContext(FormatLockOperationId(lockId, threadId)))
                        baseOperationsPerformer.WriteThread(newLockMetadata.MainRowKey(), newThreshold, threadId, lockTtl);
                    using (metrics.WriteLockMetadata2.NewContext(FormatLockOperationId(lockId, threadId)))
                        baseOperationsPerformer.WriteLockMetadata(newLockMetadata, lockMetadata.Timestamp);
                    using (metrics.WriteThread2.NewContext(FormatLockOperationId(lockId, threadId)))
                        baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, lockTtl.Multiply(10));
                }
            }
            return result;
        }

        [NotNull]
        private LockAttemptResult RunBattle([NotNull] LockMetadata lockMetadata, [NotNull] string threadId, long newThreshold)
        {
            string[] items;
            using (metrics.SearchThreads1.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
                items = baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold);
            if(items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if(items.Length > 1)
            {
                if(items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }
            string[] beforeOurWriteShades;
            using (metrics.SearchThreads2.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
                beforeOurWriteShades = baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.PreviousThreshold);
            if(beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            using (metrics.WriteThread3.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
                baseOperationsPerformer.WriteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId, lockTtl);
            string[] shades;
            using (metrics.SearchThreads3.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
                shades = baseOperationsPerformer.SearchThreads(lockMetadata.ShadowRowKey(), lockMetadata.PreviousThreshold);
            if(shades.Length == 1)
            {
                using (metrics.SearchThreads4.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
                    items = baseOperationsPerformer.SearchThreads(lockMetadata.MainRowKey(), lockMetadata.PreviousThreshold);
                if(items.Length == 0)
                {
                    using (metrics.WriteThread4.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
                        baseOperationsPerformer.WriteThread(lockMetadata.MainRowKey(), newThreshold, threadId, lockTtl);
                    using (metrics.DeleteThread1.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
                        baseOperationsPerformer.DeleteThread(lockMetadata.ShadowRowKey(), newThreshold, threadId);
                    return LockAttemptResult.Success();
                }
            }
            using (metrics.DeleteThread2.NewContext(FormatLockOperationId(lockMetadata.LockId, threadId)))
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
            baseOperationsPerformer.DeleteThread(lockMetadata.MainRowKey(), lockMetadata.GetPreviousThreshold(), threadId);
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

        private long NewThreshold(long previousThreshold)
        {
            return Math.Max(timestampProvider.GetNowTicks(), previousThreshold + 1);
        }

        private static string FormatLockOperationId(string lockId, string threadId)
        {
            return string.Format("lockId: {0}, threadId: {1}", lockId, threadId);
        }

        private readonly TimeSpan singleOperationTimeout;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly int changeLockRowThreshold;
        private readonly ITimestampProvider timestampProvider;
        private readonly CassandraBaseLockOperationsPerformer baseOperationsPerformer;
        private readonly RemoteLockerMetricsBenchTmp metrics;
    }
}