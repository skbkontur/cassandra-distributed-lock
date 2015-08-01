namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal class LockMetadata
    {
        public string LockRowId { get; set; }
        public int LockCount { get; set; }

        /*
         * This is optimization properties for repeating locks (locks that used several times).
         * According to CASSANDRA-5514, we can skip processing many SSTables during get_slice request using "min/max columns" optimization.
         * In lock implementation, we doing get_slice request on same row as many times as TryLock calls. 
         * And we scanning all the row, processing old SSTables and tombstones.
         * 
         * But after each successfull lock we can store some threshold and use it in future for formatting column name where we store threadId 
         * (column name of form {threshold}:{threadId} instead of just {threadId}). 
         * And so we can avoid scanning all the row and scan only columns >= {threshold} thereby decreasing the number of processed old SSTables and tombstones
         * during get_slice request.
         */
        public LockOwner PreviousLockOwner { get; set; }
        public LockOwner CurrentLockOwner { get; set; }
    }

    internal class LockOwner
    {
        public string ThreadId { get; set; }
        public long LockRowThreshold { get; set; }
    }
}