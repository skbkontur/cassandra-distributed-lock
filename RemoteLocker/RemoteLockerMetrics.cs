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
            TryLockAttemptsPerRequest = Context.Histogram("TryLockAttemptsPerRequest", Unit.Events);
            TryLockAttemptsRate = Context.Meter("TryLockAttemptsRate", Unit.Events);
            SleepTimeTotalMeter = Context.Meter("SleepTimeTotalMeter", new Unit("ms"));
            SleepTimeRate = Context.Histogram("SleepTimeRate", new Unit("ms"));
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
        public Histogram TryLockAttemptsPerRequest { get; private set; }
        public Meter TryLockAttemptsRate { get; private set; }
        public Meter SleepTimeTotalMeter { get; private set; }
        public Histogram SleepTimeRate { get; private set; }
    }

    public class RemoteLockerMetricsBenchTmp
    {
        public RemoteLockerMetricsBenchTmp()
        {
            Context = Metric.Context("RemoteLocker").Context("BenchTmp");
            TryGetLockMetadata1 = Context.Timer("TryGetLockMetadata1", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            ThreadAlive = Context.Timer("ThreadAlive", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            RunBattle = Context.Timer("RunBattle", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            TryGetLockMetadata2 = Context.Timer("TryGetLockMetadata2", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            WriteLockMetadata1 = Context.Timer("WriteLockMetadata1", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            WriteThread1 = Context.Timer("WriteThread1", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            WriteLockMetadata2 = Context.Timer("WriteLockMetadata2", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            WriteThread2 = Context.Timer("WriteThread2", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            SearchThreads1 = Context.Timer("SearchThreads1", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            SearchThreads2 = Context.Timer("SearchThreads2", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            WriteThread3 = Context.Timer("WriteThread3", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            WriteThread4 = Context.Timer("WriteThread4", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            SearchThreads3 = Context.Timer("SearchThreads3", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            SearchThreads4 = Context.Timer("SearchThreads4", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            DeleteThread1 = Context.Timer("DeleteThread1", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
            DeleteThread2 = Context.Timer("DeleteThread2", Unit.Calls, SamplingType.FavourRecent, TimeUnit.Minutes);
        }

        public MetricsContext Context { get; private set; }
        public Timer TryGetLockMetadata1 { get; private set; }
        public Timer ThreadAlive { get; private set; }
        public Timer RunBattle { get; private set; }
        public Timer TryGetLockMetadata2 { get; private set; }
        public Timer WriteLockMetadata1 { get; private set; }
        public Timer WriteThread1 { get; private set; }
        public Timer WriteLockMetadata2 { get; private set; }
        public Timer WriteThread2 { get; private set; }
        public Timer SearchThreads1 { get; private set; }
        public Timer SearchThreads2 { get; private set; }
        public Timer WriteThread3 { get; private set; }
        public Timer WriteThread4 { get; private set; }
        public Timer SearchThreads3 { get; private set; }
        public Timer SearchThreads4 { get; private set; }
        public Timer DeleteThread1 { get; private set; }
        public Timer DeleteThread2 { get; private set; }
    }
}