using Metrics;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker
{
    public static class RemoteLockerMetrics
    {
        public static readonly MetricsContext Context = Metric.Context("RemoteLocker");
        public static readonly Timer LockOp = Context.Timer("Lock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Timer TryGetLockOp = Context.Timer("TryGetLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Timer TryAcquireLockOp = Context.Timer("TryAcquireLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Timer ReleaseLockOp = Context.Timer("ReleaseLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Timer KeepLockAliveOp = Context.Timer("KeepLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Timer CassandraImplTryLockOp = Context.Timer("CassandraImpl.TryLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Timer CassandraImplRelockOp = Context.Timer("CassandraImpl.Relock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Timer CassandraImplUnlockOp = Context.Timer("CassandraImpl.Unlock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
        public static readonly Meter FreezeEvents = Context.Meter("FreezeEvents", Unit.Events, TimeUnit.Hours);
    }
}