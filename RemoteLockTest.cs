using System;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

using log4net;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockTest : RemoteLockTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            lockCreator = container.Get<RemoteLockCreator>();
            logger = LogManager.GetLogger(typeof(RemoteLockTest));
        }

        [Test /*, Ignore("Очень жирный тест")*/]
        public void StressTest()
        {
            DoTestIncrementDecrementLock(30, 60000, true);
        }

        [Test /*, Ignore("Очень жирный тест")*/]
        public void StressTestWithoutLocalRivalOptimization()
        {
            DoTestIncrementDecrementLock(30, 60000, false);
        }

        [Test]
        public void TestIncrementDecrementLock()
        {
            DoTestIncrementDecrementLock(10, 10000, true);
        }

        [Test]
        public void TestIncrementDecrementWithoutLocalRivalOptimization()
        {
            DoTestIncrementDecrementLock(10, 10000, false);
        }

        private void DoTestIncrementDecrementLock(int threadCount, int timeInterval, bool localRivalOptimization)
        {
            useLocalRivalOptimization = localRivalOptimization;
            for(var i = 0; i < threadCount; i++)
                AddThread(IncrementDecrementAction);
            RunThreads(timeInterval);
            JoinThreads();
        }

        private void IncrementDecrementAction(Random random)
        {
            try
            {
                var remoteLock = useLocalRivalOptimization ? lockCreator.Lock(lockId) : lockCreator.LockWithoutLocalRivalOptimization(lockId);
                using(remoteLock)
                {
                    logger.Info("MakeLock with threadId: " + remoteLock.ThreadId);
                    CheckLocks(remoteLock.ThreadId);
                    Assert.AreEqual(0, ReadX());
                    logger.Info("Increment");
                    Interlocked.Increment(ref x);
                    logger.Info("Decrement");
                    Interlocked.Decrement(ref x);
                }
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private int ReadX()
        {
            return Interlocked.CompareExchange(ref x, 0, 0);
        }

        private void CheckLocks(string threadId)
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