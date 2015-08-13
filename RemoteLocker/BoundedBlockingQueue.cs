using System.Collections.Concurrent;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker
{
    public class BoundedBlockingQueue<T> : BlockingCollection<T>
    {
        public BoundedBlockingQueue(int maxQueueSize)
            : base(new ConcurrentQueue<T>(), maxQueueSize)
        {
        }
    }
}