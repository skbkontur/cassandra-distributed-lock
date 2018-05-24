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
            LockOp = Context.Timer("Lock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
            TryGetLockOp = Context.Timer("TryGetLock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
            TryAcquireLockOp = Context.Timer("TryAcquireLock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
            ReleaseLockOp = Context.Timer("ReleaseLock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
            KeepLockAliveOp = Context.Timer("KeepLock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
            CassandraImplTryLockOp = Context.Timer("CassandraImpl.TryLock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
            CassandraImplRelockOp = Context.Timer("CassandraImpl.Relock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
            CassandraImplUnlockOp = Context.Timer("CassandraImpl.Unlock", Unit.Calls, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
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