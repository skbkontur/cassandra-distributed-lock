using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock
{
    public class NewLockMetadata
    {
        public NewLockMetadata([NotNull] string lockId, [NotNull] string lockRowId, int lockCount, long threshold, [NotNull] string ownerThreadId)
        {
            LockId = lockId;
            LockRowId = lockRowId;
            LockCount = lockCount;
            Threshold = threshold;
            OwnerThreadId = ownerThreadId;
        }

        [NotNull]
        public string LockId { get; }

        [NotNull]
        public string LockRowId { get; }

        public int LockCount { get; }

        public long Threshold { get; }

        [NotNull]
        public string OwnerThreadId { get; }

        public override string ToString()
        {
            return $"LockId: {LockId}, LockRowId: {LockRowId}, LockCount: {LockCount}, Threshold: {Threshold}, OwnerThreadId: {OwnerThreadId}";
        }
    }
}