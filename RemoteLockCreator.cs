using System;
using System.Threading;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class RemoteLockCreator : IRemoteLockCreator
    {
        public RemoteLockCreator(IRemoteLockLocalManager remoteLockLocalManager)
        {
            this.remoteLockLocalManager = remoteLockLocalManager;
        }

        public IRemoteLock Lock(string lockId)
        {
            var threadId = Guid.NewGuid().ToString();
            var random = new Random(threadId.GetHashCode());
            while(true)
            {
                string concurrentThreadId;
                var remoteLock = remoteLockLocalManager.TryAcquireLock(lockId, threadId, out concurrentThreadId);
                if(remoteLock != null)
                    return remoteLock;
                var longSleep = random.Next(1000);
                logger.WarnFormat("Поток {0} не смог взять блокировку {1}, потому что поток {2} владеет ей в данный момент. Засыпаем на {3} миллисекунд.", threadId, lockId, concurrentThreadId, longSleep);
                Thread.Sleep(longSleep);
            }
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            string concurrentThreadId;
            var threadId = Guid.NewGuid().ToString();
            remoteLock = remoteLockLocalManager.TryAcquireLock(lockId, threadId, out concurrentThreadId);
            return remoteLock != null;
        }

        private readonly IRemoteLockLocalManager remoteLockLocalManager;
        private readonly ILog logger = LogManager.GetLogger(typeof(RemoteLockCreator));
    }
}