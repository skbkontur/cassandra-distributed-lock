using System;

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
            lockRepository = new CassandraLockRepository(baseOperationsPerformer);
            changeLockRowThreshold = settings.ChangeLockRowThreshold;
        }

        public TimeSpan KeepLockAliveInterval { get { return keepLockAliveInterval; } }

        public LockAttemptResult TryLock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);

            var result = lockRepository.TryLock(lockMetadata, threadId, lockTtl, singleOperationTimeout + lockTtl);
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

        private readonly CassandraLockRepository lockRepository;
        private readonly TimeSpan singleOperationTimeout;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly int changeLockRowThreshold;
        private readonly CassandraBaseLockOperationsPerformer baseOperationsPerformer;
    }
}