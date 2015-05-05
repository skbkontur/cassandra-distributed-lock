namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public interface IRemoteLockLocalManager
    {
        IRemoteLock TryAcquireLock(string lockId, string threadId, out string concurrentThreadId);
        void ReleaseLock(string lockId, string threadId);
    }
}