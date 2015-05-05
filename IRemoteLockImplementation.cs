using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public interface IRemoteLockImplementation
    {
        TimeSpan KeepLockAliveInterval { get; }
        LockAttemptResult TryLock(string lockId, string threadId);
        void Unlock(string lockId, string threadId);
        void Relock(string lockId, string threadId);
    }
}