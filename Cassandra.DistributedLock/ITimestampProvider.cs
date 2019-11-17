namespace SkbKontur.Cassandra.DistributedLock
{
    public interface ITimestampProvider
    {
        long GetNowTicks();
    }
}