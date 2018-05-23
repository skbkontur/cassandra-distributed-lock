namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public enum LockAttemptStatus
    {
        Success,
        AnotherThreadIsOwner,
        ConcurrentAttempt
    }
}