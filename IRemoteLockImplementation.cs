namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public interface IRemoteLockImplementation
    {
        LockAttemptResult TryLock(string lockId, string threadId);
        void Unlock(string lockId, string threadId);
        void Relock(string lockId, string threadId);
    }
}