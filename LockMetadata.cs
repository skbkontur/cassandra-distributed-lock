using System;

using JetBrains.Annotations;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class LockMetadata
    {
        public LockMetadata([NotNull] string lockId, [NotNull] string lockRowId, int lockCount, long? previousThreshold, [CanBeNull] string probableOwnerThreadId, long timestamp)
        {
            LockId = lockId;
            LockRowId = lockRowId;
            LockCount = lockCount;
            PreviousThreshold = previousThreshold;
            ProbableOwnerThreadId = probableOwnerThreadId;
            Timestamp = timestamp;
        }

        [NotNull]
        public string LockId { get; private set; }

        [NotNull]
        public string LockRowId { get; private set; }

        public int LockCount { get; private set; }

        /// <summary>
        ///     This is optimization property for repeating locks (locks that used several times).
        ///     According to CASSANDRA-5514, we can skip processing many SSTables during get_slice request using "min/max columns" optimization.
        ///     In lock implementation, we doing get_slice request on same row as many times as TryLock calls.
        ///     And we scanning all the row, processing old SSTables and tombstones.
        ///     
        ///     But after each successfull lock we can store some threshold and use it in future for formatting column name where we store threadId
        ///     (column name of form {threshold}:{threadId} instead of just {threadId}).
        ///     And so we can avoid scanning all the row and scan only columns >= {threshold} thereby decreasing the number of processed old SSTables and tombstones
        ///     during get_slice request.
        /// </summary>
        public long? PreviousThreshold { get; private set; }

        public long GetPreviousThreshold()
        {
            if(!PreviousThreshold.HasValue)
                throw new InvalidOperationException(string.Format("PreviousThreshold is not set for: {0}", this));
            return PreviousThreshold.Value;
        }

        /// <summary>
        ///     This is optimization property for long locks.
        ///     Thread that doesn't owns lock tries to get lock periodically.
        ///     Without this property it leads to get_slice operation, which probably leads to scanning tombstones and reading sstables.
        ///     But we can just check is it true that ProbableOwnerThreadId still owns lock and avoid get_slice in many cases.
        /// </summary>
        [CanBeNull]
        public string ProbableOwnerThreadId { get; private set; }

        [NotNull]
        public string GetProbableOwnerThreadId()
        {
            if(string.IsNullOrEmpty(ProbableOwnerThreadId))
                throw new InvalidOperationException(string.Format("ProbableOwnerThreadId is not set for: {0}", this));
            return ProbableOwnerThreadId;
        }

        public long Timestamp { get; private set; }

        public override string ToString()
        {
            return string.Format("LockId: {0}, LockRowId: {1}, LockCount: {2}, PreviousThreshold: {3}, ProbableOwnerThreadId: {4}, timestamp: {5}",
                                 LockId, LockRowId, LockCount, PreviousThreshold, ProbableOwnerThreadId, Timestamp);
        }
    }
}