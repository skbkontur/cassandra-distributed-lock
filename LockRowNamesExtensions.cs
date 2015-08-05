namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal static class LockRowNamesExtensions
    {
        public static string ToMainRowKey(this string lockRowId)
        {
            return "Main_" + lockRowId;
        }

        public static string ToShadowRowKey(this string lockRowId)
        {
            return "Shade_" + lockRowId;
        }

        public static string ToLockMetadataRowKey(this string lockId)
        {
            return "Metadata_" + lockId;
        }
    }
}