using System;
using System.Threading;

using GroboContainer.Core;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockWithFailedCassandraTest : RemoteLockAndWeakLockTestBase
    {
        [Test]
        public void TestIncrementDecrementLock()
        {
            DoTestIncrementDecrementLock(10, 10000, true);
        }

        [Test]
        public void TestIncrementDecrementLockWithoutLocalRivalOptimization()
        {
            DoTestIncrementDecrementLock(10, 10000, false);
        }

        protected override void ConfigureContainer(IContainer c)
        {
            base.ConfigureContainer(c);
            c.Configurator.ForAbstraction<ICassandraCluster>().UseInstances(new FailedCassandraCluster(c.Get<CassandraCluster>));
        }
    }

    public class RemoteLockAndWeakLockTest : RemoteLockAndWeakLockTestBase
    {
        [Test, Ignore("Очень жирный тест")]
        public void StressTest()
        {
            DoTestIncrementDecrementLock(60, 60000, true);
        }

        [Test]
        public void TestIncrementDecrementLock()
        {
            DoTestIncrementDecrementLock(10, 10000, true);
        }

        [Test]
        public void TestIncrementDecrementLockWithoutLocalRivalOptimization()
        {
            DoTestIncrementDecrementLock(10, 10000, false);
        }
    }

    public abstract class RemoteLockAndWeakLockTestBase : RemoteLockTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            lockCreator = container.Get<RemoteLockCreator>();
            logger = LogManager.GetLogger(typeof(RemoteLockTest));
        }

        protected void DoTestIncrementDecrementLock(int threadCount, int timeInterval, bool localRivalOptimization)
        {
            useLocalRivalOptimization = localRivalOptimization;
            for(var i = 0; i < threadCount / 2; i++)
                AddThread(IncrementDecrementActionLock);
            for(var i = threadCount / 2; i < threadCount; i++)
                AddThread(IncrementDecrementActionWeakLock);
            RunThreads(timeInterval);
            JoinThreads();

            //проверяем, что после всего мы в какой-то момент сможем-таки взять лок
            Assert.That(!WeakRemoteLock.CheckLocalLockUsed(lockId), "После остановки всех потоков осталась локальная блокировка");
        }

        protected void IncrementDecrementActionLock(Random random)
        {
            try
            {
                var remoteLock = useLocalRivalOptimization ? lockCreator.Lock(lockId) : lockCreator.LockWithoutLocalRivalOptimization(lockId);
                using(remoteLock)
                {
                    Thread.Sleep(random.Next(5000));
                    logger.Info("MakeLock with threadId: " + remoteLock.ThreadId);
                    CheckLocks(remoteLock.ThreadId);
                    Assert.AreEqual(0, ReadX());
                    logger.Info("Increment");
                    Interlocked.Increment(ref x);
                    logger.Info("Decrement");
                    Interlocked.Decrement(ref x);
                }
            }
            catch(FailedCassanraClusterException e)
            {
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        protected void IncrementDecrementActionWeakLock(Random random)
        {
            try
            {
                IRemoteLock remoteLock;
                if(useLocalRivalOptimization)
                {
                    if(!lockCreator.TryGetLock(lockId, out remoteLock))
                        return;
                }
                else
                {
                    if(!lockCreator.TryGetLockWithoutLocalRivalOptimization(lockId, out remoteLock))
                        return;
                }
                using(remoteLock)
                {
                    Thread.Sleep(random.Next(5000));
                    logger.Info("MakeLock with threadId: " + remoteLock.ThreadId);
                    CheckLocks(remoteLock.ThreadId);
                    Assert.AreEqual(0, ReadX());
                    logger.Info("Increment");
                    Interlocked.Increment(ref x);
                    logger.Info("Decrement");
                    Interlocked.Decrement(ref x);
                }
            }
            catch (FailedCassanraClusterException e)
            {
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        protected int ReadX()
        {
            return Interlocked.CompareExchange(ref x, 0, 0);
        }

        protected void CheckLocks(string threadId)
        {
            try
            {
                var lr = container.Get<IRemoteLockImplementation>() as CassandraRemoteLockImplementation;
                var locks = lr.GetLockThreads(lockId);
                logger.Info("Locks: " + string.Join(", ", locks));
                Assert.That(locks.Length <= 1, "Too many locks");
                Assert.That(locks.Length == 1);
                Assert.AreEqual(threadId, locks[0]);
                var lockShades = lr.GetShadeThreads(lockId);
                logger.Info("LockShades: " + string.Join(", ", lockShades));
            }
            catch (FailedCassanraClusterException e)
            {
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private volatile bool useLocalRivalOptimization;

        private int x;
        private RemoteLockCreator lockCreator;
        private ILog logger;

        private const string lockId = "IncDecLock";
    }
    
}