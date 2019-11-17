using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock
{
    public interface IRemoteLockCreator
    {
        [NotNull]
        IRemoteLock Lock([NotNull] string lockId);

        bool TryGetLock([NotNull] string lockId, out IRemoteLock remoteLock);
    }
}