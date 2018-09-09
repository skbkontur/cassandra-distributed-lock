using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class DefaultTimestampProvider : ITimestampProvider
    {
        public long GetNowTicks()
        {
            return DateTime.UtcNow.Ticks;
        }
    }
}