using Metrics;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class RemoteLockerMetrics
    {
        public RemoteLockerMetrics()
        {
            var context = Metric.Context("RemoteLocker");
            TryAcquireLockOp = context.Timer("TryAcquireLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
            ReleaseLockOp = context.Timer("ReleaseLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
            KeepLockOp = context.Timer("KeepLock", Unit.Calls, SamplingType.LongTerm, TimeUnit.Minutes);
            FreezeEvents = context.Meter("FreezeEvents", Unit.Events, TimeUnit.Hours);
        }

        public Timer TryAcquireLockOp { get; private set; }
        public Timer ReleaseLockOp { get; private set; }
        public Timer KeepLockOp { get; private set; }
        public Meter FreezeEvents { get; private set; }
    }
}