using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock
{
    public class LockAttemptResult
    {
        private LockAttemptResult(LockAttemptStatus status, [CanBeNull] string ownerId)
        {
            Status = status;
            OwnerId = ownerId;
        }

        [NotNull]
        public static LockAttemptResult Success()
        {
            return new LockAttemptResult(LockAttemptStatus.Success, null);
        }

        [NotNull]
        public static LockAttemptResult AnotherOwner([NotNull] string ownerId)
        {
            return new LockAttemptResult(LockAttemptStatus.AnotherThreadIsOwner, ownerId);
        }

        [NotNull]
        public static LockAttemptResult ConcurrentAttempt()
        {
            return new LockAttemptResult(LockAttemptStatus.ConcurrentAttempt, null);
        }

        public LockAttemptStatus Status { get; }

        [CanBeNull]
        public string OwnerId { get; }
    }
}