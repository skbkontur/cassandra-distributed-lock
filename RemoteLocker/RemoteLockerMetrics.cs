using Metrics;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker
{
    public class RemoteLockerMetrics
    {
        public RemoteLockerMetrics(string keyspaceName)
        {
            Context = Metric.Context("RemoteLocker");
            if(!string.IsNullOrEmpty(keyspaceName))
                Context = Context.Context(keyspaceName);
            LockOp = Context.Timer("Lock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            TryGetLockOp = Context.Timer("TryGetLock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            TryAcquireLockOp = Context.Timer("TryAcquireLock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            ReleaseLockOp = Context.Timer("ReleaseLock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            KeepLockAliveOp = Context.Timer("KeepLock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            CassandraImplTryLockOp = Context.Timer("CassandraImpl.TryLock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            CassandraImplRelockOp = Context.Timer("CassandraImpl.Relock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            CassandraImplUnlockOp = Context.Timer("CassandraImpl.Unlock", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            FreezeEvents = Context.Meter("FreezeEvents", Unit.Events, TimeUnit.Hours);
        }

        public MetricsContext Context { get; private set; }
        public Timer LockOp { get; private set; }
        public Timer TryGetLockOp { get; private set; }
        public Timer TryAcquireLockOp { get; private set; }
        public Timer ReleaseLockOp { get; private set; }
        public Timer KeepLockAliveOp { get; private set; }
        public Timer CassandraImplTryLockOp { get; private set; }
        public Timer CassandraImplRelockOp { get; private set; }
        public Timer CassandraImplUnlockOp { get; private set; }
        public Meter FreezeEvents { get; private set; }
    }
}