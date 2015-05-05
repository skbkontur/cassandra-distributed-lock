using System.Collections.Concurrent;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class BoundedBlockingQueue<T> : BlockingCollection<T>
    {
        public BoundedBlockingQueue(int maxQueueSize)
            : base(new ConcurrentQueue<T>(), maxQueueSize)
        {
        }
    }
}