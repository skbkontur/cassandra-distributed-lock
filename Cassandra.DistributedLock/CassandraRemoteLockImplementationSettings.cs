using System;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.DistributedLock
{
    public class CassandraRemoteLockImplementationSettings
    {
        public CassandraRemoteLockImplementationSettings(
            [NotNull] ITimestampProvider timestampProvider,
            [NotNull] string keyspaceName,
            [NotNull] string columnFamilyName,
            TimeSpan lockTtl,
            TimeSpan lockMetadataTtl,
            TimeSpan keepLockAliveInterval,
            int changeLockRowThreshold)
        {
            TimestampProvider = timestampProvider;
            KeyspaceName = keyspaceName;
            ColumnFamilyName = columnFamilyName;
            LockTtl = lockTtl;
            LockMetadataTtl = lockMetadataTtl;
            KeepLockAliveInterval = keepLockAliveInterval;
            if (changeLockRowThreshold <= 0)
                throw new ArgumentException("ChangeRowThreshold must be positive integer", nameof(changeLockRowThreshold));
            ChangeLockRowThreshold = changeLockRowThreshold;
        }

        [NotNull]
        public ITimestampProvider TimestampProvider { get; }

        [NotNull]
        public string KeyspaceName { get; }

        [NotNull]
        public string ColumnFamilyName { get; }

        public TimeSpan LockTtl { get; }
        public TimeSpan LockMetadataTtl { get; }
        public TimeSpan KeepLockAliveInterval { get; }
        public int ChangeLockRowThreshold { get; }

        [NotNull]
        public static CassandraRemoteLockImplementationSettings Default([NotNull] string keyspaceName, [NotNull] string columnFamilyName)
        {
            return new CassandraRemoteLockImplementationSettings(new DefaultTimestampProvider(),
                                                                 keyspaceName,
                                                                 columnFamilyName,
                                                                 lockTtl : TimeSpan.FromMinutes(3),
                                                                 lockMetadataTtl : TimeSpan.FromDays(7),
                                                                 keepLockAliveInterval : TimeSpan.FromSeconds(10),
                                                                 changeLockRowThreshold : 1000);
        }
    }
}