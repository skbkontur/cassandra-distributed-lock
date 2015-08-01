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
            lockRepository = new CassandraLockRepository(cassandraCluster, serializer, settings.ColumnFamilyFullName);
        }

        public TimeSpan KeepLockAliveInterval { get { return keepLockAliveInterval; } }

        public LockAttemptResult TryLock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);

            var result = lockRepository.TryLock(lockMetadata, threadId, lockTtl, singleOperationTimeout + lockTtl);
            if(result.Status == LockAttemptStatus.Success)
            {
                if(lockMetadata.LockCount > 1000)
                {
                    var newLockMetadata = GenerateNewLockMetadata();

                    lockRepository.UpdateLockRowTtl(lockMetadata, threadId, singleOperationTimeout.Multiply(3));
                    lockRepository.LockRowUnSafe(newLockMetadata, threadId, singleOperationTimeout.Multiply(2) + lockTtl);
                    lockRepository.WriteLockMetadata(lockId, newLockMetadata);
                    lockRepository.UpdateLockRowTtl(lockMetadata, threadId, TimeSpan.FromDays(7));

                    return LockAttemptResult.Success();
                }

                lockRepository.IncrementLockCount(lockId, lockMetadata);
            }

            return result;
        }

        public void Unlock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            lockRepository.UnlockRow(lockMetadata, threadId);
        }

        public void Relock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            lockRepository.RelockRow(lockMetadata, threadId, lockTtl);
        }

        public string[] GetLockThreads(string lockId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            return lockRepository.GetThreadsInLockRow(lockMetadata);
        }

        public string[] GetShadeThreads(string lockId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);
            return lockRepository.GetShadowThreadsInLockRow(lockMetadata);
        }

        private LockMetadata GetOrCreateLockMetadata(string lockId)
        {
            var result = lockRepository.GetLockMetadata(lockId, lockId);
            return result ?? new LockMetadata {LockCount = 0, LockRowId = lockId};
        }

        private static LockMetadata GenerateNewLockMetadata()
        {
            return new LockMetadata
                {
                    LockCount = 1,
                    LockRowId = Guid.NewGuid().ToString()
                };
        }

        private readonly CassandraLockRepository lockRepository;
        private readonly TimeSpan singleOperationTimeout;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
    }
}