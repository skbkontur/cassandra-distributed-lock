using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal class DefaultTimestampProvider : ITimestampProvider
    {
        public long GetNowTicks()
        {
            return DateTime.UtcNow.Ticks;
        }
    }
}