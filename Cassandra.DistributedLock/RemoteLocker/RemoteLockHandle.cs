using System.Threading;

namespace SkbKontur.Cassandra.DistributedLock.RemoteLocker
{
    public class RemoteLockHandle : IRemoteLock
    {
        public RemoteLockHandle(string lockId, string threadId, CancellationToken lockAliveToken, RemoteLocker remoteLocker)
        {
            LockId = lockId;
            ThreadId = threadId;
            LockAliveToken = lockAliveToken;
            this.remoteLocker = remoteLocker;
        }

        public string LockId { get; }
        public string ThreadId { get; }
        public CancellationToken LockAliveToken { get; }

        public void Dispose()
        {
            remoteLocker.ReleaseLock(LockId, ThreadId);
        }

        private readonly RemoteLocker remoteLocker;
    }
}