using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

using JetBrains.Annotations;

using SkbKontur.Cassandra.TimeBasedUuid;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.DistributedLock.RemoteLocker
{
    public class RemoteLocker : IDisposable, IRemoteLockCreator
    {
        public RemoteLocker(IRemoteLockImplementation remoteLockImplementation, RemoteLockerMetrics metrics, ILog logger)
        {
            this.remoteLockImplementation = remoteLockImplementation;
            this.metrics = metrics;
            this.logger = logger.ForContext("CassandraDistributedLock");
            lockTtl = remoteLockImplementation.LockTtl;
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
                    var longSleep = ThreadLocalRandom.Instance.Next(1000);
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

        [SuppressMessage("ReSharper", "ParameterOnlyUsedForPreconditionCheck.Local")]
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
                var initialHeartbeatMoment = Timestamp.Now;
                using (metrics.CassandraImplTryLockOp.NewContext(FormatLockOperationId(lockId, threadId)))
                    lockAttempt = remoteLockImplementation.TryLock(lockId, threadId);
                switch (lockAttempt.Status)
                {
                case LockAttemptStatus.Success:
                    rivalThreadId = null;
                    var remoteLockState = new RemoteLockState(lockId, threadId, initialHeartbeatMoment);
                    if (!remoteLocksById.TryAdd(lockId, remoteLockState))
                        throw new InvalidOperationException($"RemoteLocker state is corrupted. lockId: {lockId}, threadId: {threadId}, remoteLocksById[lockId]: {remoteLockState}");
                    remoteLocksQueue.Add(remoteLockState);
                    return new RemoteLockHandle(lockId, threadId, remoteLockState.LockAliveTokenSource.Token, this);
                case LockAttemptStatus.AnotherThreadIsOwner:
                    rivalThreadId = lockAttempt.OwnerId;
                    return null;
                case LockAttemptStatus.ConcurrentAttempt:
                    var shortSleep = ThreadLocalRandom.Instance.Next(50 * (int)Math.Exp(Math.Min(attempt++, 5)));
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
                throw new InvalidOperationException($"RemoteLocker state is corrupted. lockId: {lockId}, threadId: {threadId}, remoteLocksById[lockId]: {remoteLockState}");
            Unlock(remoteLockState);
        }

        private void Unlock(RemoteLockState remoteLockState)
        {
            lock (remoteLockState)
            {
                remoteLockState.IsAlive = false;
                remoteLockState.LockAliveTokenSource.Dispose();
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

        private void KeepLockAlive([NotNull] RemoteLockState remoteLockState)
        {
            TimeSpan? timeToSleep = null;
            lock (remoteLockState)
            {
                if (!remoteLockState.IsAlive)
                    return;

                var utcNow = Timestamp.Now;
                var nextKeepAliveMoment = remoteLockState.HeartbeatMoment.Add(keepLockAliveInterval);
                if (utcNow < nextKeepAliveMoment)
                    timeToSleep = nextKeepAliveMoment - utcNow;
            }

            if (timeToSleep.HasValue)
                Thread.Sleep(timeToSleep.Value);

            lock (remoteLockState)
            {
                if (!remoteLockState.IsAlive)
                    return;

                var relocked = TryRelock(remoteLockState);

                if (!relocked)
                {
                    remoteLockState.LockAliveTokenSource.Cancel();
                    return;
                }

                if (!remoteLocksQueue.IsAddingCompleted)
                    remoteLocksQueue.Add(remoteLockState);
            }
        }

        private bool TryRelock([NotNull] RemoteLockState remoteLockState)
        {
            var attempt = 1;
            while (true)
            {
                var nextHeartbeatMoment = Timestamp.Now;
                if (remoteLockState.HeartbeatMoment.Add(lockTtl.Multiply(0.5)) < nextHeartbeatMoment)
                {
                    logger.Error("KeepLockAlive() freeze is detected on attempt #{0}. Signal LockAliveToken to prevent possible lock collision for: {1}", attempt, remoteLockState);
                    return false;
                }

                try
                {
                    using (metrics.CassandraImplRelockOp.NewContext(remoteLockState.ToString()))
                    {
                        var relocked = remoteLockImplementation.TryRelock(remoteLockState.LockId, remoteLockState.ThreadId);

                        if (relocked)
                            remoteLockState.HeartbeatMoment = nextHeartbeatMoment;
                        else
                            logger.Error("Cannot relock on attempt #{0}. Possible lock metadata corruption for: {1}", attempt, remoteLockState);

                        return relocked;
                    }
                }
                catch (Exception e)
                {
                    var shortSleep = ThreadLocalRandom.Instance.Next(50 * (int)Math.Exp(Math.Min(attempt++, 5)));
                    logger.Warn(e, "remoteLockImplementation.TryRelock() attempt #{0} failed for: {1}. Will sleep for {2} ms", attempt, remoteLockState, shortSleep);
                    Thread.Sleep(shortSleep);
                }
            }
        }

        private volatile bool isDisposed;
        private readonly Thread remoteLocksKeeperThread;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan keepLockAliveInterval;
        private readonly TimeSpan lockOperationWarnThreshold;
        private readonly IRemoteLockImplementation remoteLockImplementation;
        private readonly RemoteLockerMetrics metrics;
        private readonly ILog logger;
        private readonly ConcurrentDictionary<string, RemoteLockState> remoteLocksById = new ConcurrentDictionary<string, RemoteLockState>();
        private readonly BoundedBlockingQueue<RemoteLockState> remoteLocksQueue = new BoundedBlockingQueue<RemoteLockState>(int.MaxValue);

        private class RemoteLockState
        {
            public RemoteLockState(string lockId, string threadId, [NotNull] Timestamp heartbeatMoment)
            {
                LockId = lockId;
                ThreadId = threadId;
                HeartbeatMoment = heartbeatMoment;
                LockAliveTokenSource = new CancellationTokenSource();
                IsAlive = true;
            }

            public string LockId { get; }

            public string ThreadId { get; }

            public bool IsAlive { get; set; }

            [NotNull]
            public Timestamp HeartbeatMoment { get; set; }

            public CancellationTokenSource LockAliveTokenSource { get; }

            public override string ToString()
            {
                return $"LockId: {LockId}, ThreadId: {ThreadId}, IsAlive: {IsAlive}, HeartbeatMoment: {HeartbeatMoment}";
            }
        }
    }
}