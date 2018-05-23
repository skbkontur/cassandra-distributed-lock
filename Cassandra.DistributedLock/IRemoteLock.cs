using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public interface IRemoteLock : IDisposable
    {
        string LockId { get; }
        string ThreadId { get; }
    }
}