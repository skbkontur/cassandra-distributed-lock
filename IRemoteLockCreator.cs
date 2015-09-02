using JetBrains.Annotations;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public interface IRemoteLockCreator
    {
        [NotNull]
        IRemoteLock Lock([NotNull] string lockId);

        bool TryGetLock([NotNull] string lockId, out IRemoteLock remoteLock);
    }
}