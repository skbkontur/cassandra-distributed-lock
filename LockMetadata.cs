namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal class LockMetadata
    {
        public string LockRowId { get; set; }
        public int LockCount { get; set; }
    }
}