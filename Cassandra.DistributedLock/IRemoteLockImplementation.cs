using System;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock
{
    public interface IRemoteLockImplementation
    {
        TimeSpan KeepLockAliveInterval { get; }

        [NotNull]
        LockAttemptResult TryLock([NotNull] string lockId, [NotNull] string threadId);

        bool TryUnlock([NotNull] string lockId, [NotNull] string threadId);
        bool TryRelock([NotNull] string lockId, [NotNull] string threadId);
    }
}