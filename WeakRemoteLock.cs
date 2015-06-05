using System;
using System.Collections.Concurrent;
using System.Threading;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class WeakRemoteLock : IRemoteLock
    {
        public WeakRemoteLock(IRemoteLockImplementation remoteLockImplementation, string lockId, out string concurrentThreadId, string threadId = null, bool localRivalsOptimization = true)
        {
            this.remoteLockImplementation = remoteLockImplementation;
            keepLockAliveInterval = remoteLockImplementation.KeepLockAliveInterval;
            this.lockId = lockId;
            threadId = string.IsNullOrEmpty(threadId) ? Guid.NewGuid().ToString() : threadId;
            this.threadId = threadId;
            var random = new Random(Guid.NewGuid().GetHashCode());
            var attempt = 1;

            try
            {
                if(localRivalsOptimization)
                {
                    var localRival = localWeakRemoteLocks.GetOrAdd(lockId, this);
                    if(localRival != this)
                    {
                        concurrentThreadId = localRival.ThreadId;
                        return;
                    }
                }

                while(true)
                {
                    var lockAttempt = remoteLockImplementation.TryLock(lockId, threadId);
                    switch(lockAttempt.Status)
                    {
                    case LockAttemptStatus.Success:
                        stopEvent = new ManualResetEvent(false);
                        thread = new Thread(UpdateLock);
                        thread.Start();
                        concurrentThreadId = null;
                        return;
                    case LockAttemptStatus.AnotherThreadIsOwner:
                        concurrentThreadId = lockAttempt.OwnerId;
                        RemoveMeFromLocalStorage();
                        return;
                    default:
                        var shortSleep = random.Next(50 * (int)Math.Exp(Math.Min(attempt, 10)));
                        attempt++;
                        logger.WarnFormat("Поток {0} не смог взять блокировку {1} из-за конкуррентной попытки других потоков. Засыпаем на {2} миллисекунд.", threadId, lockId, shortSleep);
                        Thread.Sleep(shortSleep);
                        break;
                    }
                }
            }
            catch(Exception e)
            {
                logger.Error("Exception in the WeakRemoteLock constructor. Try to dispose", e);
                Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            try
            {
                if(stopEvent != null)
                {
                    stopEvent.Set();
                    thread.Join();
                    stopEvent.Dispose();
                    remoteLockImplementation.Unlock(lockId, threadId);
                }
            }
            finally
            {
                RemoveMeFromLocalStorage();
            }
        }

        public string LockId { get { return lockId; } }
        public string ThreadId { get { return threadId; } }

        /// <summary>
        ///     Только для тестов
        /// </summary>
        /// <param name="lockId"></param>
        /// <returns></returns>
        public static bool CheckLocalLockUsed(string lockId)
        {
            return localWeakRemoteLocks.ContainsKey(lockId);
        }

        private void RemoveMeFromLocalStorage()
        {
            while(true)
            {
                WeakRemoteLock @lock;
                if(!(localWeakRemoteLocks.TryGetValue(lockId, out @lock) && @lock == this))
                    break;
                if(localWeakRemoteLocks.TryRemove(lockId, out @lock))
                    break;
                const int sleepTime = 100;
                logger.WarnFormat("Не смогли удалить собственную блокировку {0} из локального хранилища. Засыпаем на {1} миллисекунд", lockId, sleepTime);
                Thread.Sleep(sleepTime);
            }
        }

        private void UpdateLock()
        {
            var lastTicks = DateTime.UtcNow;
            while(true)
            {
                try
                {
                    var diff = DateTime.UtcNow - lastTicks;
                    if(diff > TimeSpan.FromSeconds(50))
                    {
                        logger.Error(string.Format("Difference between updates too large: {0}s. Update stopped", diff));
                        return;
                    }
                    remoteLockImplementation.Relock(lockId, threadId);
                    if(stopEvent.WaitOne(keepLockAliveInterval)) break;
                    diff = DateTime.UtcNow - lastTicks;
                    if(diff > TimeSpan.FromSeconds(30))
                        logger.WarnFormat(string.Format("Difference between updates too large: {0}s", diff));

                    lastTicks = DateTime.UtcNow;
                }
                catch(Exception e)
                {
                    logger.Error("Ошибка во время удержания блокировки.", e);
                }
            }
        }

        private static readonly ConcurrentDictionary<string, WeakRemoteLock> localWeakRemoteLocks = new ConcurrentDictionary<string, WeakRemoteLock>();
        private readonly ILog logger = LogManager.GetLogger(typeof(WeakRemoteLock));
        private readonly string lockId;
        private readonly string threadId;
        private readonly IRemoteLockImplementation remoteLockImplementation;
        private readonly Thread thread;
        private readonly ManualResetEvent stopEvent;
        private readonly TimeSpan keepLockAliveInterval;
    }
}