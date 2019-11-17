using System.Collections.Concurrent;

namespace SkbKontur.Cassandra.DistributedLock.RemoteLocker
{
    public class BoundedBlockingQueue<T> : BlockingCollection<T>
    {
        public BoundedBlockingQueue(int maxQueueSize)
            : base(new ConcurrentQueue<T>(), maxQueueSize)
        {
        }
    }
}