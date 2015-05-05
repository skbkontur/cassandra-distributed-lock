namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class RemoteLockHandle : IRemoteLock
    {
        public RemoteLockHandle(string lockId, string threadId, IRemoteLockLocalManager remoteLockLocalManager)
        {
            LockId = lockId;
            ThreadId = threadId;
            this.remoteLockLocalManager = remoteLockLocalManager;
        }

        public void Dispose()
        {
            remoteLockLocalManager.ReleaseLock(LockId, ThreadId);
        }

        public string LockId { get; private set; }
        public string ThreadId { get; private set; }

        private readonly IRemoteLockLocalManager remoteLockLocalManager;
    }
}