using System;

using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class CassandraRemoteLockImplementationSettings
    {
        public CassandraRemoteLockImplementationSettings(
            ColumnFamilyFullName columnFamilyFullName, 
            TimeSpan lockTtl, 
            TimeSpan keepLockAliveInterval, 
            int changeLockRowThreshold)
        {
            ColumnFamilyFullName = columnFamilyFullName;
            LockTtl = lockTtl;
            KeepLockAliveInterval = keepLockAliveInterval;
            if (changeLockRowThreshold <= 0)
                throw new ArgumentException("ChangeRowThreshold must be positive integer", "changeLockRowThreshold");
            ChangeLockRowThreshold = changeLockRowThreshold;
        }

        public ColumnFamilyFullName ColumnFamilyFullName { get; private set; }
        public TimeSpan LockTtl { get; private set; }
        public TimeSpan KeepLockAliveInterval { get; private set; }
        public int ChangeLockRowThreshold { get; private set; }

        public static CassandraRemoteLockImplementationSettings Default(ColumnFamilyFullName columnFamilyFullName)
        {
            return new CassandraRemoteLockImplementationSettings(columnFamilyFullName, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(10), 1000);
        }
    }
}