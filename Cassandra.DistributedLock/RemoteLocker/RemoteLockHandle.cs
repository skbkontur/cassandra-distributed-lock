using System.Threading;

namespace SkbKontur.Cassandra.DistributedLock.RemoteLocker
{
    public class RemoteLockHandle : IRemoteLock
    {
        public RemoteLockHandle(string lockId, string threadId, CancellationToken expirationToken, RemoteLocker remoteLocker)
        {
            LockId = lockId;
            ThreadId = threadId;
            ExpirationToken = expirationToken;
            this.remoteLocker = remoteLocker;
        }

        public string LockId { get; }

        public string ThreadId { get; }

        public CancellationToken ExpirationToken { get; }

        public void Dispose()
        {
            remoteLocker.ReleaseLock(LockId, ThreadId);
        }

        private readonly RemoteLocker remoteLocker;
    }
}