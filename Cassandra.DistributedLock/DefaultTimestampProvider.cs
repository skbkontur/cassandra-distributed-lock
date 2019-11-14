using SkbKontur.Cassandra.TimeBasedUuid;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class DefaultTimestampProvider : ITimestampProvider
    {
        public long GetNowTicks()
        {
            return Timestamp.Now.Ticks;
        }
    }
}