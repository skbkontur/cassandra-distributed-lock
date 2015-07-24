using System;

using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class CassandraRemoteLockImplementationSettings
    {
        public ColumnFamilyFullName ColumnFamilyFullName { get; set; }
        public TimeSpan LockTtl { get; set; }
        public TimeSpan KeepLockAliveInterval { get; set; }

        public static CassandraRemoteLockImplementationSettings Default(ColumnFamilyFullName columnFamilyFullName)
        {
            return new CassandraRemoteLockImplementationSettings
                {
                    ColumnFamilyFullName = columnFamilyFullName,
                    LockTtl = TimeSpan.FromMinutes(3),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(10),
                };
        }
    }
}