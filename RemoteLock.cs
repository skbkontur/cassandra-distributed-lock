using System;
using System.Threading;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class RemoteLock : IRemoteLock
    {
        public RemoteLock(IRemoteLockImplementation remoteLockImplementation, string lockId, bool localRivalOptimization = true)
        {
            this.lockId = lockId;
            threadId = Guid.NewGuid().ToString();
            var random = new Random(Guid.NewGuid().GetHashCode());

            while(true)
            {
                string concurrentThreadId;
                weakRemoteLock = new WeakRemoteLock(remoteLockImplementation, lockId, out concurrentThreadId, threadId, localRivalOptimization);
                if(string.IsNullOrEmpty(concurrentThreadId))
                    break;

                var longSleep = random.Next(1000);
                logger.WarnFormat("Поток {0} не смог взять блокировку {1}, потому что поток {2} владеет ей в данный момент. Засыпаем на {3} миллисекунд.", threadId, lockId, concurrentThreadId, longSleep);
                Thread.Sleep(longSleep);
            }
        }

        public void Dispose()
        {
            weakRemoteLock.Dispose();
        }

        public string LockId { get { return lockId; } }
        public string ThreadId { get { return threadId; } }
        private readonly ILog logger = LogManager.GetLogger(typeof(RemoteLock));
        private readonly WeakRemoteLock weakRemoteLock;
        private readonly string lockId;
        private readonly string threadId;
    }
}