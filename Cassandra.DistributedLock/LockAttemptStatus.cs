namespace SkbKontur.Cassandra.DistributedLock
{
    public enum LockAttemptStatus
    {
        Success,
        AnotherThreadIsOwner,
        ConcurrentAttempt
    }
}