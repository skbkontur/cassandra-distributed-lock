using System;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal class CassandraLockRepository
    {
        public CassandraLockRepository(CassandraBaseLockOperationsPerformer baseOperationsPerformer)
        {
            this.baseOperationsPerformer = baseOperationsPerformer;
        }

        public LockAttemptResult TryLock(LockMetadata lockMetadata, string threadId, TimeSpan lockTtl, TimeSpan ttl)
        {
            var items = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold);
            if(items.Length == 1)
                return items[0] == threadId ? LockAttemptResult.Success() : LockAttemptResult.AnotherOwner(items[0]);
            if(items.Length > 1)
            {
                if(items.Any(s => s == threadId))
                    throw new Exception("Lock unknown exception");
                return LockAttemptResult.AnotherOwner(items[0]);
            }

            var beforeOurWriteShades = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold);
            if(beforeOurWriteShades.Length > 0)
                return LockAttemptResult.ConcurrentAttempt();
            baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold, threadId, lockTtl);
            var shades = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold);
            if(shades.Length == 1)
            {
                items = baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold);
                if(items.Length == 0)
                {
                    baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.CurrentThreshold, threadId, ttl);
                    baseOperationsPerformer.DeleteThread(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold, threadId);
                    return LockAttemptResult.Success();
                }
            }
            baseOperationsPerformer.DeleteThread(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold, threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        private readonly CassandraBaseLockOperationsPerformer baseOperationsPerformer;
    }
}