using System;

namespace SkbKontur.Cassandra.DistributedLock
{
    public interface IRemoteLock : IDisposable
    {
        string LockId { get; }
        string ThreadId { get; }
    }
}