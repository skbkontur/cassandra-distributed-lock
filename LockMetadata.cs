namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal class LockMetadata
    {
        public LockMetadata(
            string lockId,
            string lockRowId, 
            int lockCount, 
            long? previousThreshold,
            string probableOwnerThreadId)
        {
            LockId = lockId;
            LockRowId = lockRowId;
            LockCount = lockCount;
            PreviousThreshold = previousThreshold;
            ProbableOwnerThreadId = probableOwnerThreadId;
        }

        public string LockRowId { get; private set; }

        public int LockCount { get; private set; }

        public string LockId { get; private set; }

        /*
         * This is optimization property for repeating locks (locks that used several times).
         * According to CASSANDRA-5514, we can skip processing many SSTables during get_slice request using "min/max columns" optimization.
         * In lock implementation, we doing get_slice request on same row as many times as TryLock calls. 
         * And we scanning all the row, processing old SSTables and tombstones.
         * 
         * But after each successfull lock we can store some threshold and use it in future for formatting column name where we store threadId 
         * (column name of form {threshold}:{threadId} instead of just {threadId}). 
         * And so we can avoid scanning all the row and scan only columns >= {threshold} thereby decreasing the number of processed old SSTables and tombstones
         * during get_slice request.
         */
        public long? PreviousThreshold { get; private set; }

        /*
         * This is optimization property for long locks.
         * Thread that doesn't owns lock tries to get lock periodically.
         * Without this property it leads to get_slice operation, which probably leads to scanning tombstones and reading sstables.
         * But we can just check is it true that ProbableOwnerThreadId still owns lock and avoid get_slice in many cases.
         */
        public string ProbableOwnerThreadId { get; private set; }
    }
}