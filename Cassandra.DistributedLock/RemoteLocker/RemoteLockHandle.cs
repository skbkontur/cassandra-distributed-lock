namespace SkbKontur.Cassandra.DistributedLock.RemoteLocker
{
    public class RemoteLockHandle : IRemoteLock
    {
        public RemoteLockHandle(string lockId, string threadId, RemoteLocker remoteLocker)
        {
            LockId = lockId;
            ThreadId = threadId;
            this.remoteLocker = remoteLocker;
        }

        public void Dispose()
        {
            remoteLocker.ReleaseLock(LockId, ThreadId);
        }

        public string LockId { get; }
        public string ThreadId { get; }

        private readonly RemoteLocker remoteLocker;
    }
}