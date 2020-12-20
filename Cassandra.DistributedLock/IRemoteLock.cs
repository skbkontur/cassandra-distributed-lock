using System;
using System.Threading;

namespace SkbKontur.Cassandra.DistributedLock
{
    public interface IRemoteLock : IDisposable
    {
        string LockId { get; }
        string ThreadId { get; }
        CancellationToken ExpirationToken { get; }
    }
}