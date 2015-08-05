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

        public string[] GetThreadsInLockRow(LockMetadata lockMetadata)
        {
            return baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold);
        }

        public string[] GetShadowThreadsInLockRow(LockMetadata lockMetadata)
        {
            return baseOperationsPerformer.SeatchThreads(lockMetadata.LockRowId.ToShadowRowKey(), lockMetadata.CurrentThreshold);
        }

        public void UnlockRow(LockMetadata lockMetadata, string threadId)
        {
            baseOperationsPerformer.DeleteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold, threadId);
        }

        public void RelockRow(LockMetadata lockMetadata, string threadId, TimeSpan lockTtl)
        {
            baseOperationsPerformer.WriteThread(lockMetadata.LockRowId.ToMainRowKey(), lockMetadata.PreviousThreshold, threadId, lockTtl);
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
            WriteThreadToRow(lockMetadata.CurrentThreshold, lockMetadata.LockRowId.ToShadowRowKey(), threadId, lockTtl);
            var shades = GetShadowThreadsInLockRow(lockMetadata);
            if(shades.Length == 1)
            {
                items = GetThreadsInLockRow(lockMetadata);
                if(items.Length == 0)
                {
                    WriteThreadToRow(lockMetadata.CurrentThreshold, lockMetadata.LockRowId.ToMainRowKey(), threadId, ttl);
                    DeleteThreadFromRow(lockMetadata.CurrentThreshold, lockMetadata.LockRowId.ToShadowRowKey(), threadId);
                    return LockAttemptResult.Success();
                }
            }
            DeleteThreadFromRow(lockMetadata.CurrentThreshold, lockMetadata.LockRowId.ToShadowRowKey(), threadId);
            return LockAttemptResult.ConcurrentAttempt();
        }

        public void UpdateLockRowTtl(LockMetadata lockMetadata, string threadId, TimeSpan ttl)
        {
            WriteThreadToRow(lockMetadata.CurrentThreshold, lockMetadata.LockRowId.ToMainRowKey(), threadId, ttl);
        }

        public void WriteLockMetadata(string lockId, LockMetadata lockMetadata)
        {
            baseOperationsPerformer.WriteLockMetadata(lockId.ToLockMetadataRowKey(), lockMetadata);
        }

        public LockMetadata GetLockMetadata(string lockId, string defaultLockRowId)
        {
            return baseOperationsPerformer.GetLockMetadata(lockId.ToLockMetadataRowKey(), defaultLockRowId);
        }

        private void WriteThreadToRow(long? threshold, string rowName, string threadId, TimeSpan ttl)
        {
            baseOperationsPerformer.WriteThread(rowName, threshold, threadId, ttl);
        }

        private void DeleteThreadFromRow(long? threshold, string rowName, string threadId)
        {
            baseOperationsPerformer.DeleteThread(rowName, threshold, threadId);
        }

        private readonly CassandraBaseLockOperationsPerformer baseOperationsPerformer;
    }
}