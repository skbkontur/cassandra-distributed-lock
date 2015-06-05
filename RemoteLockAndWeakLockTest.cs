using System;
using System.Threading;

using GroboContainer.Core;

using log4net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    public class RemoteLockWithFailedCassandraTest : RemoteLockAndWeakLockTestBase
    {
        [TestCase(true, LocalRivalOptimization.Disabled)]
        [TestCase(true, LocalRivalOptimization.Enabled)]
        [TestCase(false, LocalRivalOptimization.Disabled)]
        [TestCase(false, LocalRivalOptimization.Enabled)]
        public void TestIncrementDecrementLock(bool useSingleLockKeeperThread, LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(useSingleLockKeeperThread, 10, TimeSpan.FromSeconds(10), localRivalOptimization);
        }

        protected override void ConfigureContainer(IContainer c)
        {
            base.ConfigureContainer(c);
            c.Configurator.ForAbstraction<ICassandraCluster>().UseInstances(new FailedCassandraCluster(c.Get<CassandraCluster>));
        }
    }

    public class RemoteLockAndWeakLockTest : RemoteLockAndWeakLockTestBase
    {
        [Ignore("Очень жирный тест")]
        [TestCase(true, LocalRivalOptimization.Disabled)]
        [TestCase(true, LocalRivalOptimization.Enabled)]
        [TestCase(false, LocalRivalOptimization.Disabled)]
        [TestCase(false, LocalRivalOptimization.Enabled)]
        public void StressTest(bool useSingleLockKeeperThread, LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(useSingleLockKeeperThread, 60, TimeSpan.FromSeconds(60), localRivalOptimization);
        }

        [TestCase(true, LocalRivalOptimization.Disabled)]
        [TestCase(true, LocalRivalOptimization.Enabled)]
        [TestCase(false, LocalRivalOptimization.Disabled)]
        [TestCase(false, LocalRivalOptimization.Enabled)]
        public void TestIncrementDecrementLock(bool useSingleLockKeeperThread, LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(useSingleLockKeeperThread, 10, TimeSpan.FromSeconds(10), localRivalOptimization);
        }
    }

    public abstract class RemoteLockAndWeakLockTestBase : RemoteLockTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            logger = LogManager.GetLogger(typeof(RemoteLockTest));
            remoteLockImplementation = (CassandraRemoteLockImplementation)container.Get<IRemoteLockImplementation>();
        }

        protected void DoTestIncrementDecrementLock(bool useSingleLockKeeperThread, int threadCount, TimeSpan runningTimeInterval, LocalRivalOptimization localRivalOptimization)
        {
            var remoteLockCreators = PrepareRemoteLockCreators(useSingleLockKeeperThread, threadCount, localRivalOptimization, remoteLockImplementation);

            for(var i = 0; i < threadCount / 2; i++)
                AddThread(IncrementDecrementActionLock, remoteLockCreators[i]);
            for(var i = threadCount / 2; i < threadCount; i++)
                AddThread(IncrementDecrementActionWeakLock, remoteLockCreators[i]);
            RunThreads(runningTimeInterval);
            JoinThreads();

            CheckLockIsNotAcquiredLocally(useSingleLockKeeperThread, remoteLockCreators, lockId);
            DisposeRemoteLockCreators(useSingleLockKeeperThread, remoteLockCreators);
        }

        private void IncrementDecrementActionLock(IRemoteLockCreator lockCreator, Random random)
        {
            try
            {
                var remoteLock = lockCreator.Lock(lockId);
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
            catch(FailedCassanraClusterException)
            {
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private void IncrementDecrementActionWeakLock(IRemoteLockCreator lockCreator, Random random)
        {
            try
            {
                IRemoteLock remoteLock;
                if(!lockCreator.TryGetLock(lockId, out remoteLock))
                    return;
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
            catch(FailedCassanraClusterException)
            {
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
                var locks = remoteLockImplementation.GetLockThreads(lockId);
                logger.Info("Locks: " + string.Join(", ", locks));
                Assert.That(locks.Length <= 1, "Too many locks");
                Assert.That(locks.Length == 1);
                Assert.AreEqual(threadId, locks[0]);
                var lockShades = remoteLockImplementation.GetShadeThreads(lockId);
                logger.Info("LockShades: " + string.Join(", ", lockShades));
            }
            catch(FailedCassanraClusterException)
            {
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private const string lockId = "IncDecLock";
        private int x;
        private ILog logger;
        private CassandraRemoteLockImplementation remoteLockImplementation;
    }
}