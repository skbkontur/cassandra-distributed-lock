using System;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class CassandraRemoteLockImplementation : IRemoteLockImplementation
    {
        public CassandraRemoteLockImplementation(ICassandraCluster cassandraCluster, ISerializer serializer, ColumnFamilyFullName columnFamilyFullName)
        {
            var connectionParameters = cassandraCluster.RetrieveColumnFamilyConnection(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName).GetConnectionParameters();
            singleOperationTimeout = TimeSpan.FromMilliseconds(connectionParameters.Attempts * connectionParameters.Timeout);
            lockTtl = TimeSpan.FromMinutes(3);
            keepLockAliveInterval = TimeSpan.FromSeconds(15);
            lockRepository = new CassandraLockRepository(cassandraCluster, serializer, lockTtl, columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
        }

        public TimeSpan KeepLockAliveInterval { get { return keepLockAliveInterval; } }

        public LockAttemptResult TryLock(string lockId, string threadId)
        {
            var lockMetadata = GetOrCreateLockMetadata(lockId);

            var result = lockRepository.TryLock(lockMetadata.LockRowId, threadId, singleOperationTimeout + lockTtl);
            if(result.Status == LockAttemptStatus.Success)
            {
                if(lockMetadata.LockCount > 1000)
                {
                    var newLockMetadata = GenerateNewLockMetadata();

                    lockRepository.UpdateLockRowTTL(lockMetadata.LockRowId, threadId, singleOperationTimeout.Multiply(3));
                    lockRepository.LockRowUnSafe(newLockMetadata.LockRowId, threadId, singleOperationTimeout.Multiply(2) + lockTtl);
                    lockRepository.WriteLockMetadata(lockId, newLockMetadata);
                    lockRepository.UpdateLockRowTTL(lockMetadata.LockRowId, threadId, TimeSpan.FromDays(7));

                    return LockAttemptResult.Success();
                }

                lockRepository.IncrementLockCount(lockId, lockMetadata);
            }

            return result;
        }

        public void Unlock(string lockId, string threadId)
        {
            var lockReference = GetOrCreateLockMetadata(lockId);
            lockRepository.UnlockRow(lockReference.LockRowId, threadId);
        }

        public void Relock(string lockId, string threadId)
        {
            var lockReference = GetOrCreateLockMetadata(lockId);
            lockRepository.RelockRow(lockReference.LockRowId, threadId);
        }

        public string[] GetLockThreads(string lockId)
        {
            var lockReference = GetOrCreateLockMetadata(lockId);
            return lockRepository.GetThreadsInLockRow(lockReference.LockRowId);
        }

        public string[] GetShadeThreads(string lockId)
        {
            var lockReference = GetOrCreateLockMetadata(lockId);
            return lockRepository.GetShadowThreadsInLockRow(lockReference.LockRowId);
        }

        private LockMetadata GetOrCreateLockMetadata(string lockId)
        {
            var result = lockRepository.GetLockMetadata(lockId, lockId);
            return result ?? new LockMetadata {LockCount = 0, LockRowId = lockId};
        }

        private LockMetadata GenerateNewLockMetadata()
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