using System;

using JetBrains.Annotations;

using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class CassandraRemoteLockImplementationSettings
    {
        public CassandraRemoteLockImplementationSettings(
            [NotNull] ITimestampProvider timestampProvider,
            [NotNull] ColumnFamilyFullName columnFamilyFullName,
            TimeSpan lockTtl,
            TimeSpan keepLockAliveInterval,
            int changeLockRowThreshold)
        {
            TimestampProvider = timestampProvider;
            ColumnFamilyFullName = columnFamilyFullName;
            LockTtl = lockTtl;
            KeepLockAliveInterval = keepLockAliveInterval;
            if(changeLockRowThreshold <= 0)
                throw new ArgumentException("ChangeRowThreshold must be positive integer", "changeLockRowThreshold");
            ChangeLockRowThreshold = changeLockRowThreshold;
        }

        [NotNull]
        public ITimestampProvider TimestampProvider { get; private set; }

        [NotNull]
        public ColumnFamilyFullName ColumnFamilyFullName { get; private set; }

        public TimeSpan LockTtl { get; private set; }
        public TimeSpan KeepLockAliveInterval { get; private set; }
        public int ChangeLockRowThreshold { get; private set; }

        [NotNull]
        public static CassandraRemoteLockImplementationSettings Default([NotNull] ColumnFamilyFullName columnFamilyFullName)
        {
            return new CassandraRemoteLockImplementationSettings(new DefaultTimestampProvider(), columnFamilyFullName, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(10), 1000);
        }
    }
}