using GroboContainer.Infection;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [IgnoredImplementation]
    public class RemoteLockCreatorWthoutLocalRivalOptimization : IRemoteLockCreator
    {
        public RemoteLockCreatorWthoutLocalRivalOptimization(RemoteLockCreator remoteLockCreator)
        {
            this.remoteLockCreator = remoteLockCreator;
        }

        public IRemoteLock Lock(string lockId)
        {
            return remoteLockCreator.LockWithoutLocalRivalOptimization(lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            return remoteLockCreator.TryGetLockWithoutLocalRivalOptimization(lockId, out remoteLock);
        }

        private readonly RemoteLockCreator remoteLockCreator;
    }
}