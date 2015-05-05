using System;
using System.Collections.Generic;
using System.Threading;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class RemoteLockLocalManager : IDisposable, IRemoteLockLocalManager
    {
        public RemoteLockLocalManager(IRemoteLockImplementation remoteLockImplementation)
        {
            this.remoteLockImplementation = remoteLockImplementation;
            keepLockAliveInterval = remoteLockImplementation.KeepLockAliveInterval;
            remoteLocksKeeperThread = new Thread(KeepRemoteLocksAlive)
                {
                    IsBackground = true,
                    Name = "remoteLocksKeeper",
                };
            remoteLocksKeeperThread.Start();
        }

        public void Dispose()
        {
            if(isDisposed)
                return;
            remoteLocksQueue.CompleteAdding();
            remoteLocksKeeperThread.Join();
            remoteLocksQueue.Dispose();
            isDisposed = true;
        }

        public IRemoteLock TryAcquireLock(string lockId, string threadId, out string concurrentThreadId)
        {
            ValidateArgs(lockId, threadId);
            lock(locker)
            {
                EnsureNotDisposed();
                return DoTryAcquireLock(lockId, threadId, out concurrentThreadId);
            }
        }

        public void ReleaseLock(string lockId, string threadId)
        {
            ValidateArgs(lockId, threadId);
            lock(locker)
            {
                EnsureNotDisposed();
                DoReleaseLock(lockId, threadId);
            }
        }

        [Obsolete("Только для тестов")]
        public bool CheckLockIsAcquiredLocally(string lockId)
        {
            lock(locker)
            {
                EnsureNotDisposed();
                return remoteLocksById.ContainsKey(lockId);
            }
        }

        private static void ValidateArgs(string lockId, string threadId)
        {
            if(string.IsNullOrEmpty(lockId))
                throw new InvalidOperationException("lockId is empty");
            if(string.IsNullOrEmpty(threadId))
                throw new InvalidOperationException("threadId is empty");
        }

        private void EnsureNotDisposed()
        {
            if(isDisposed)
                throw new ObjectDisposedException("RemoteLockLocalManager is already disposed");
        }

        private IRemoteLock DoTryAcquireLock(string lockId, string threadId, out string rivalThreadId)
        {
            RemoteLockState rival;
            if(remoteLocksById.TryGetValue(lockId, out rival))
            {
                rivalThreadId = rival.ThreadId;
                return null;
            }
            var attempt = 1;
            while(true)
            {
                var lockAttempt = remoteLockImplementation.TryLock(lockId, threadId);
                switch(lockAttempt.Status)
                {
                case LockAttemptStatus.Success:
                    rivalThreadId = null;
                    var remoteLockState = new RemoteLockState(lockId, threadId, locker, DateTime.UtcNow.Add(keepLockAliveInterval));
                    remoteLocksById.Add(lockId, remoteLockState);
                    remoteLocksQueue.Add(remoteLockState);
                    return new RemoteLockHandle(lockId, threadId, this);
                case LockAttemptStatus.AnotherThreadIsOwner:
                    rivalThreadId = lockAttempt.OwnerId;
                    return null;
                case LockAttemptStatus.ConcurrentAttempt:
                    var shortSleep = random.Next(50 * (int)Math.Exp(Math.Min(attempt++, 10)));
                    logger.WarnFormat("Поток {0} не смог взять блокировку {1} из-за конкуррентной попытки других потоков. Засыпаем на {2} миллисекунд.", threadId, lockId, shortSleep);
                    Thread.Sleep(shortSleep);
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Invalid LockAttemptStatus: {0}", lockAttempt.Status));
                }
            }
        }

        private void DoReleaseLock(string lockId, string threadId)
        {
            RemoteLockState remoteLockState;
            if(!remoteLocksById.TryGetValue(lockId, out remoteLockState) || remoteLockState.ThreadId != threadId)
                throw new InvalidOperationException(string.Format("RemoteLockLocalManager state is corrupted. lockId: {0}, threaId: {1}, remoteLocksById[lockId]: {2}", lockId, threadId, remoteLockState));
            remoteLockState.NextKeepAliveMoment = null;
            remoteLocksById.Remove(lockId);
            remoteLockImplementation.Unlock(lockId, threadId);
        }

        private void KeepRemoteLocksAlive()
        {
            try
            {
                while(!remoteLocksQueue.IsCompleted)
                {
                    RemoteLockState remoteLockState;
                    if(remoteLocksQueue.TryTake(out remoteLockState, Timeout.Infinite))
                        ProcessRemoteLocksQueueItem(remoteLockState);
                }
            }
            catch(Exception e)
            {
                logger.Fatal("RemoteLocksKeeper thread failed", e);
            }
        }

        private void ProcessRemoteLocksQueueItem(RemoteLockState remoteLockState)
        {
            TimeSpan? timeToSleep = null;
            lock(remoteLockState.ManagerLocker)
            {
                var nextKeepAliveMoment = remoteLockState.NextKeepAliveMoment;
                if(!nextKeepAliveMoment.HasValue)
                    return;
                var utcNow = DateTime.UtcNow;
                if(utcNow < nextKeepAliveMoment)
                    timeToSleep = nextKeepAliveMoment - utcNow;
            }
            if(timeToSleep.HasValue)
                Thread.Sleep(timeToSleep.Value);
            lock(remoteLockState.ManagerLocker)
            {
                if(!remoteLockState.NextKeepAliveMoment.HasValue)
                    return;
                try
                {
                    remoteLockImplementation.Relock(remoteLockState.LockId, remoteLockState.ThreadId);
                }
                catch(Exception e)
                {
                    logger.Error(string.Format("Failed to relock: {0}", remoteLockState), e);
                }
                if(!remoteLocksQueue.IsAddingCompleted)
                {
                    remoteLockState.NextKeepAliveMoment = DateTime.UtcNow.Add(keepLockAliveInterval);
                    remoteLocksQueue.Add(remoteLockState);
                }
            }
        }

        private volatile bool isDisposed;
        private readonly Thread remoteLocksKeeperThread;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly IRemoteLockImplementation remoteLockImplementation;
        private readonly object locker = new object();
        private readonly Random random = new Random(Guid.NewGuid().GetHashCode());
        private readonly ILog logger = LogManager.GetLogger(typeof(RemoteLockLocalManager));
        private readonly Dictionary<string, RemoteLockState> remoteLocksById = new Dictionary<string, RemoteLockState>();
        private readonly BoundedBlockingQueue<RemoteLockState> remoteLocksQueue = new BoundedBlockingQueue<RemoteLockState>(int.MaxValue);

        private class RemoteLockState
        {
            public RemoteLockState(string lockId, string threadId, object managerLocker, DateTime nextKeepAliveMoment)
            {
                LockId = lockId;
                ThreadId = threadId;
                ManagerLocker = managerLocker;
                NextKeepAliveMoment = nextKeepAliveMoment;
            }

            public string LockId { get; private set; }
            public string ThreadId { get; private set; }
            public object ManagerLocker { get; private set; }
            public DateTime? NextKeepAliveMoment { get; set; }

            public override string ToString()
            {
                return string.Format("LockId: {0}, ThreadId: {1}, NextKeepAliveMoment: {2}", LockId, ThreadId, NextKeepAliveMoment);
            }
        }
    }
}