namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal static class LockRowNamesExtensions
    {
        public static string MainRowKey(this LockMetadata lockMetadata)
        {
            return "Main_" + lockMetadata.LockRowId;
        }

        public static string ShadowRowKey(this LockMetadata lockMetadata)
        {
            return "Shade_" + lockMetadata.LockRowId;
        }
        
        public static string ToLockMetadataRowKey(this string lockId)
        {
            return "Metadata_" + lockId;
        }
    }
}