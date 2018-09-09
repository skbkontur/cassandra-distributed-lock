namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public interface ITimestampProvider
    {
        long GetNowTicks();
    }
}