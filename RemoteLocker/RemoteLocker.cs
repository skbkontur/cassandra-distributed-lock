using System;
using System.Collections.Concurrent;
using System.Threading;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker
{
    public class RemoteLocker : IDisposable, IRemoteLockCreator
    {
        public RemoteLocker(IRemoteLockImplementation remoteLockImplementation, RemoteLockerMetrics metrics)
        {
            this.remoteLockImplementation = remoteLockImplementation;
            this.metrics = metrics;
            keepLockAliveInterval = remoteLockImplementation.KeepLockAliveInterval;
            lockOperationWarnThreshold = remoteLockImplementation.KeepLockAliveInterval.Multiply(2);
            remoteLocksKeeperThread = new Thread(KeepRemoteLocksAlive)
                {
                    IsBackground = true,
                    Name = "remoteLocksKeeper",
                };
            remoteLocksKeeperThread.Start();
        }

        public IRemoteLock Lock(string lockId)
        {
            var threadId = Guid.NewGuid().ToString();
            Action<TimeSpan> finalAction = elapsed =>
                {
                    if(elapsed < lockOperationWarnThreshold)
                        return;
                    metrics.FreezeEvents.Mark("Lock");
                    logger.ErrorFormat("Lock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
                };
            using(metrics.LockOp.NewContext(finalAction, FormatLockOperationId(lockId, threadId)))
            {
                while(true)
                {
                    string concurrentThreadId;
                    var remoteLock = TryAcquireLock(lockId, threadId, out concurrentThreadId);
                    if(remoteLock != null)
                        return remoteLock;
                    var longSleep = random.Next(1000);
                    logger.WarnFormat("Поток {0} не смог взять блокировку {1}, потому что поток {2} владеет ей в данный момент. Засыпаем на {3} миллисекунд.", threadId, lockId, concurrentThreadId, longSleep);
                    Thread.Sleep(longSleep);
                }
            }
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            var threadId = Guid.NewGuid().ToString();
            Action<TimeSpan> finalAction = elapsed =>
                {
                    if(elapsed < lockOperationWarnThreshold)
                        return;
                    metrics.FreezeEvents.Mark("TryGetLock");
                    logger.ErrorFormat("TryGetLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
                };
            using(metrics.TryGetLockOp.NewContext(finalAction, FormatLockOperationId(lockId, threadId)))
            {
                string concurrentThreadId;
                remoteLock = TryAcquireLock(lockId, threadId, out concurrentThreadId);
                return remoteLock != null;
            }
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

        private IRemoteLock TryAcquireLock(string lockId, string threadId, out string concurrentThreadId)
        {
            EnsureNotDisposed();
            ValidateArgs(lockId, threadId);
            Action<TimeSpan> finalAction = elapsed =>
                {
                    if(elapsed < lockOperationWarnThreshold)
                        return;
                    metrics.FreezeEvents.Mark("TryAcquireLock");
                    logger.ErrorFormat("TryAcquireLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
                };
            using(metrics.TryAcquireLockOp.NewContext(finalAction, FormatLockOperationId(lockId, threadId)))
                return DoTryAcquireLock(lockId, threadId, out concurrentThreadId);
        }

        public void ReleaseLock(string lockId, string threadId)
        {
            EnsureNotDisposed();
            ValidateArgs(lockId, threadId);
            Action<TimeSpan> finalAction = elapsed =>
                {
                    if(elapsed < lockOperationWarnThreshold)
                        return;
                    metrics.FreezeEvents.Mark("ReleaseLock");
                    logger.ErrorFormat("ReleaseLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
                };
            using(metrics.ReleaseLockOp.NewContext(finalAction, FormatLockOperationId(lockId, threadId)))
                DoReleaseLock(lockId, threadId);
        }

        [Obsolete("Только для тестов")]
        public bool CheckLockIsAcquiredLocally(string lockId)
        {
            EnsureNotDisposed();
            return remoteLocksById.ContainsKey(lockId);
        }

        private static string FormatLockOperationId(string lockId, string threadId)
        {
            return string.Format("lockId: {0}, threadId: {1}", lockId, threadId);
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
                throw new ObjectDisposedException("RemoteLocker is already disposed");
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
                LockAttemptResult lockAttempt;
                using(metrics.CassandraImplTryLockOp.NewContext(FormatLockOperationId(lockId, threadId)))
                    lockAttempt = remoteLockImplementation.TryLock(lockId, threadId);
                switch(lockAttempt.Status)
                {
                case LockAttemptStatus.Success:
                    rivalThreadId = null;
                    var remoteLockState = new RemoteLockState(lockId, threadId, DateTime.UtcNow.Add(keepLockAliveInterval));
                    if(!remoteLocksById.TryAdd(lockId, remoteLockState))
                        throw new InvalidOperationException(string.Format("RemoteLocker state is corrupted. lockId: {0}, threaId: {1}, remoteLocksById[lockId]: {2}", lockId, threadId, remoteLockState));
                    remoteLocksQueue.Add(remoteLockState);
                    return new RemoteLockHandle(lockId, threadId, this);
                case LockAttemptStatus.AnotherThreadIsOwner:
                    rivalThreadId = lockAttempt.OwnerId;
                    return null;
                case LockAttemptStatus.ConcurrentAttempt:
                    var shortSleep = random.Next(50 * (int)Math.Exp(Math.Min(attempt++, 10)));
                    logger.WarnFormat("remoteLockImplementation.TryLock() returned LockAttemptStatus.ConcurrentAttempt for lockId: {0}, threadId: {1}. Will sleep for {2} ms", lockId, threadId, shortSleep);
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
            if(!remoteLocksById.TryRemove(lockId, out remoteLockState) || remoteLockState.ThreadId != threadId)
                throw new InvalidOperationException(string.Format("RemoteLocker state is corrupted. lockId: {0}, threaId: {1}, remoteLocksById[lockId]: {2}", lockId, threadId, remoteLockState));
            Unlock(remoteLockState);
        }

        private void Unlock(RemoteLockState remoteLockState)
        {
            lock(remoteLockState)
            {
                remoteLockState.NextKeepAliveMoment = null;
                try
                {
                    using(metrics.CassandraImplUnlockOp.NewContext(remoteLockState.ToString()))
                        remoteLockImplementation.Unlock(remoteLockState.LockId, remoteLockState.ThreadId);
                }
                catch(Exception e)
                {
                    logger.Error(string.Format("remoteLockImplementation.Unlock() failed for: {0}", remoteLockState), e);
                }
            }
        }

        private void KeepRemoteLocksAlive()
        {
            try
            {
                while(!remoteLocksQueue.IsCompleted)
                {
                    RemoteLockState remoteLockState;
                    if(remoteLocksQueue.TryTake(out remoteLockState, Timeout.Infinite))
                    {
                        Action<TimeSpan> finalAction = elapsed =>
                            {
                                if(elapsed < keepLockAliveInterval + lockOperationWarnThreshold)
                                    return;
                                metrics.FreezeEvents.Mark("KeepLockAlive");
                                logger.ErrorFormat("KeepLockAlive() took {0} ms for remote lock: {1}", elapsed.TotalMilliseconds, remoteLockState);
                            };
                        using(metrics.KeepLockAliveOp.NewContext(finalAction, remoteLockState.ToString()))
                            KeepLockAlive(remoteLockState);
                    }
                }
            }
            catch(Exception e)
            {
                logger.Fatal("RemoteLocksKeeper thread failed", e);
            }
        }

        private void KeepLockAlive(RemoteLockState remoteLockState)
        {
            TimeSpan? timeToSleep = null;
            lock(remoteLockState)
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
            lock(remoteLockState)
            {
                if(!remoteLockState.NextKeepAliveMoment.HasValue)
                    return;
                Relock(remoteLockState);
                if(!remoteLocksQueue.IsAddingCompleted)
                {
                    remoteLockState.NextKeepAliveMoment = DateTime.UtcNow.Add(keepLockAliveInterval);
                    remoteLocksQueue.Add(remoteLockState);
                }
            }
        }

        private void Relock(RemoteLockState remoteLockState)
        {
            var attempt = 1;
            while(true)
            {
                try
                {
                    using(metrics.CassandraImplRelockOp.NewContext(remoteLockState.ToString()))
                        remoteLockImplementation.Relock(remoteLockState.LockId, remoteLockState.ThreadId);
                    break;
                }
                catch(Exception e)
                {
                    var shortSleep = random.Next(50 * (int)Math.Exp(Math.Min(attempt++, 10)));
                    logger.Warn(string.Format("remoteLockImplementation.Relock() failed for: {0}. Will sleep for {1} ms", remoteLockState, shortSleep), e);
                    Thread.Sleep(shortSleep);
                }
            }
        }

        private volatile bool isDisposed;
        private readonly Thread remoteLocksKeeperThread;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly TimeSpan lockOperationWarnThreshold;
        private readonly IRemoteLockImplementation remoteLockImplementation;
        private readonly RemoteLockerMetrics metrics;
        private readonly Random random = new Random(Guid.NewGuid().GetHashCode());
        private readonly ILog logger = LogManager.GetLogger(typeof(RemoteLocker));
        private readonly ConcurrentDictionary<string, RemoteLockState> remoteLocksById = new ConcurrentDictionary<string, RemoteLockState>();
        private readonly BoundedBlockingQueue<RemoteLockState> remoteLocksQueue = new BoundedBlockingQueue<RemoteLockState>(int.MaxValue);

        private class RemoteLockState
        {
            public RemoteLockState(string lockId, string threadId, DateTime nextKeepAliveMoment)
            {
                LockId = lockId;
                ThreadId = threadId;
                NextKeepAliveMoment = nextKeepAliveMoment;
            }

            public string LockId { get; private set; }
            public string ThreadId { get; private set; }
            public DateTime? NextKeepAliveMoment { get; set; }

            public override string ToString()
            {
                return string.Format("LockId: {0}, ThreadId: {1}, NextKeepAliveMoment: {2}", LockId, ThreadId, NextKeepAliveMoment);
            }
        }
    }
}