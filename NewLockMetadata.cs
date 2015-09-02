using JetBrains.Annotations;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
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
        public string LockId { get; private set; }

        [NotNull]
        public string LockRowId { get; private set; }

        public int LockCount { get; private set; }
        public long Threshold { get; private set; }

        [NotNull]
        public string OwnerThreadId { get; private set; }

        public override string ToString()
        {
            return string.Format("LockId: {0}, LockRowId: {1}, LockCount: {2}, Threshold: {3}, OwnerThreadId: {4}", LockId, LockRowId, LockCount, Threshold, OwnerThreadId);
        }
    }
}