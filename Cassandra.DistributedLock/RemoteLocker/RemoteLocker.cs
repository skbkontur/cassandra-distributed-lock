using System;
using System.Collections.Concurrent;
using System.Threading;

using JetBrains.Annotations;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker
{
    public class RemoteLocker : IDisposable, IRemoteLockCreator
    {
        public RemoteLocker(IRemoteLockImplementation remoteLockImplementation, RemoteLockerMetrics metrics, ILog logger)
        {
            this.remoteLockImplementation = remoteLockImplementation;
            this.metrics = metrics;
            this.logger = logger.ForContext("CassandraDistributedLock");
            keepLockAliveInterval = remoteLockImplementation.KeepLockAliveInterval;
            lockOperationWarnThreshold = remoteLockImplementation.KeepLockAliveInterval.Multiply(2);
            remoteLocksKeeperThread = new Thread(KeepRemoteLocksAlive)
                {
                    IsBackground = true,
                    Name = "remoteLocksKeeper",
                };
            remoteLocksKeeperThread.Start();
        }

        [NotNull]
        public IRemoteLock Lock([NotNull] string lockId)
        {
            var threadId = Guid.NewGuid().ToString();

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.Mark("Lock");
                logger.Error("Lock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.LockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
            {
                while (true)
                {
                    var remoteLock = TryAcquireLock(lockId, threadId, out var concurrentThreadId);
                    if (remoteLock != null)
                        return remoteLock;
                    var longSleep = random.Next(1000);
                    logger.Warn("Поток {0} не смог взять блокировку {1}, потому что поток {2} владеет ей в данный момент. Засыпаем на {3} миллисекунд.", threadId, lockId, concurrentThreadId, longSleep);
                    Thread.Sleep(longSleep);
                }
            }
        }

        public bool TryGetLock([NotNull] string lockId, out IRemoteLock remoteLock)
        {
            var threadId = Guid.NewGuid().ToString();

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.Mark("TryGetLock");
                logger.Error("TryGetLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.TryGetLockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
            {
                remoteLock = TryAcquireLock(lockId, threadId, out _);
                return remoteLock != null;
            }
        }

        public void Dispose()
        {
            if (isDisposed)
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

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.Mark("TryAcquireLock");
                logger.Error("TryAcquireLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.TryAcquireLockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
                return DoTryAcquireLock(lockId, threadId, out concurrentThreadId);
        }

        public void ReleaseLock(string lockId, string threadId)
        {
            EnsureNotDisposed();
            ValidateArgs(lockId, threadId);

            void FinalAction(TimeSpan elapsed)
            {
                if (elapsed < lockOperationWarnThreshold)
                    return;
                metrics.FreezeEvents.Mark("ReleaseLock");
                logger.Error("ReleaseLock() took {0} ms for lockId: {1}, threadId: {2}", elapsed.TotalMilliseconds, lockId, threadId);
            }

            using (metrics.ReleaseLockOp.NewContext(FinalAction, FormatLockOperationId(lockId, threadId)))
                DoReleaseLock(lockId, threadId);
        }

        private static string FormatLockOperationId(string lockId, string threadId)
        {
            return $"lockId: {lockId}, threadId: {threadId}";
        }

        private static void ValidateArgs(string lockId, string threadId)
        {
            if (string.IsNullOrEmpty(lockId))
                throw new InvalidOperationException("lockId is empty");
            if (string.IsNullOrEmpty(threadId))
                throw new InvalidOperationException("threadId is empty");
        }

        private void EnsureNotDisposed()
        {
            if (isDisposed)
                throw new ObjectDisposedException("RemoteLocker is already disposed");
        }

        private IRemoteLock DoTryAcquireLock(string lockId, string threadId, out string rivalThreadId)
        {
            if (remoteLocksById.TryGetValue(lockId, out var rival))
            {
                rivalThreadId = rival.ThreadId;
                return null;
            }
            var attempt = 1;
            while (true)
            {
                LockAttemptResult lockAttempt;
                using (metrics.CassandraImplTryLockOp.NewContext(FormatLockOperationId(lockId, threadId)))
                    lockAttempt = remoteLockImplementation.TryLock(lockId, threadId);
                switch (lockAttempt.Status)
                {
                case LockAttemptStatus.Success:
                    rivalThreadId = null;
                    var remoteLockState = new RemoteLockState(lockId, threadId, DateTime.UtcNow.Add(keepLockAliveInterval));
                    if (!remoteLocksById.TryAdd(lockId, remoteLockState))
                        throw new InvalidOperationException($"RemoteLocker state is corrupted. lockId: {lockId}, threaId: {threadId}, remoteLocksById[lockId]: {remoteLockState}");
                    remoteLocksQueue.Add(remoteLockState);
                    return new RemoteLockHandle(lockId, threadId, this);
                case LockAttemptStatus.AnotherThreadIsOwner:
                    rivalThreadId = lockAttempt.OwnerId;
                    return null;
                case LockAttemptStatus.ConcurrentAttempt:
                    var shortSleep = random.Next(50 * (int)Math.Exp(Math.Min(attempt++, 5)));
                    logger.Warn("remoteLockImplementation.TryLock() returned LockAttemptStatus.ConcurrentAttempt for lockId: {0}, threadId: {1}. Will sleep for {2} ms", lockId, threadId, shortSleep);
                    Thread.Sleep(shortSleep);
                    break;
                default:
                    throw new InvalidOperationException($"Invalid LockAttemptStatus: {lockAttempt.Status}");
                }
            }
        }

        private void DoReleaseLock(string lockId, string threadId)
        {
            if (!remoteLocksById.TryRemove(lockId, out var remoteLockState) || remoteLockState.ThreadId != threadId)
                throw new InvalidOperationException($"RemoteLocker state is corrupted. lockId: {lockId}, threaId: {threadId}, remoteLocksById[lockId]: {remoteLockState}");
            Unlock(remoteLockState);
        }

        private void Unlock(RemoteLockState remoteLockState)
        {
            lock (remoteLockState)
            {
                remoteLockState.NextKeepAliveMoment = null;
                try
                {
                    using (metrics.CassandraImplUnlockOp.NewContext(remoteLockState.ToString()))
                    {
                        if (!remoteLockImplementation.TryUnlock(remoteLockState.LockId, remoteLockState.ThreadId))
                            logger.Error("Cannot unlock. Possible lock metadata corruption for: {0}", remoteLockState);
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e, "remoteLockImplementation.Unlock() failed for: {0}", remoteLockState);
                }
            }
        }

        private void KeepRemoteLocksAlive()
        {
            try
            {
                while (!remoteLocksQueue.IsCompleted)
                {
                    if (remoteLocksQueue.TryTake(out var remoteLockState, Timeout.Infinite))
                    {
                        void FinalAction(TimeSpan elapsed)
                        {
                            if (elapsed < keepLockAliveInterval + lockOperationWarnThreshold)
                                return;
                            metrics.FreezeEvents.Mark("KeepLockAlive");
                            logger.Error("KeepLockAlive() took {0} ms for remote lock: {1}", elapsed.TotalMilliseconds, remoteLockState);
                        }

                        using (metrics.KeepLockAliveOp.NewContext(FinalAction, remoteLockState.ToString()))
                            KeepLockAlive(remoteLockState);
                    }
                }
            }
            catch (Exception e)
            {
                logger.Fatal(e, "RemoteLocksKeeper thread failed");
            }
        }

        private void KeepLockAlive(RemoteLockState remoteLockState)
        {
            TimeSpan? timeToSleep = null;
            lock (remoteLockState)
            {
                var nextKeepAliveMoment = remoteLockState.NextKeepAliveMoment;
                if (!nextKeepAliveMoment.HasValue)
                    return;
                var utcNow = DateTime.UtcNow;
                if (utcNow < nextKeepAliveMoment)
                    timeToSleep = nextKeepAliveMoment - utcNow;
            }
            if (timeToSleep.HasValue)
                Thread.Sleep(timeToSleep.Value);
            lock (remoteLockState)
            {
                if (!remoteLockState.NextKeepAliveMoment.HasValue)
                    return;
                var relocked = TryRelock(remoteLockState);
                if (relocked && !remoteLocksQueue.IsAddingCompleted)
                {
                    remoteLockState.NextKeepAliveMoment = DateTime.UtcNow.Add(keepLockAliveInterval);
                    remoteLocksQueue.Add(remoteLockState);
                }
            }
        }

        private bool TryRelock(RemoteLockState remoteLockState)
        {
            var attempt = 1;
            while (true)
            {
                try
                {
                    using (metrics.CassandraImplRelockOp.NewContext(remoteLockState.ToString()))
                    {
                        var relocked = remoteLockImplementation.TryRelock(remoteLockState.LockId, remoteLockState.ThreadId);
                        if (!relocked)
                            logger.Error("Cannot relock. Possible lock metadata corruption for: {0}", remoteLockState);
                        return relocked;
                    }
                }
                catch (Exception e)
                {
                    var shortSleep = random.Next(50 * (int)Math.Exp(Math.Min(attempt++, 5)));
                    logger.Warn(e, "remoteLockImplementation.Relock() failed for: {0}. Will sleep for {1} ms", remoteLockState, shortSleep);
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
        private readonly ILog logger;
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

            public string LockId { get; }
            public string ThreadId { get; }
            public DateTime? NextKeepAliveMoment { get; set; }

            public override string ToString()
            {
                return $"LockId: {LockId}, ThreadId: {ThreadId}, NextKeepAliveMoment: {NextKeepAliveMoment}";
            }
        }
    }
}