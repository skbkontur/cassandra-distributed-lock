using JetBrains.Annotations;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal static class LockRowNamesExtensions
    {
        [NotNull]
        public static string MainRowKey([NotNull] this LockMetadata lockMetadata)
        {
            return "Main_" + lockMetadata.LockRowId;
        }

        [NotNull]
        public static string MainRowKey([NotNull] this NewLockMetadata lockMetadata)
        {
            return "Main_" + lockMetadata.LockRowId;
        }

        [NotNull]
        public static string ShadowRowKey([NotNull] this LockMetadata lockMetadata)
        {
            return "Shade_" + lockMetadata.LockRowId;
        }

        [NotNull]
        public static string ToLockMetadataRowKey([NotNull] this string lockId)
        {
            return "Metadata_" + lockId;
        }
    }
}