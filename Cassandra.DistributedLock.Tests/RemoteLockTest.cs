using System;
using System.Collections.Generic;
using System.Threading;

using Cassandra.DistributedLock.Tests.Logging;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;

using Vostok.Logging.Abstractions;

namespace Cassandra.DistributedLock.Tests
{
    public class RemoteLockTest
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var cassandraCluster = new CassandraCluster(SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(), logger);
            var settings = new CassandraRemoteLockImplementationSettings(new DefaultTimestampProvider(), SingleCassandraNodeSetUpFixture.RemoteLockKeyspace, SingleCassandraNodeSetUpFixture.RemoteLockColumnFamily, TimeSpan.FromMinutes(3), TimeSpan.FromDays(30), TimeSpan.FromSeconds(5), 10);
            remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, settings);
        }

        [SetUp]
        public void SetUp()
        {
            logger.Info("Start SetUp, runningThreads = {0}", runningThreads);
            runningThreads = 0;
            isEnd = false;
            threads = new List<Thread>();
        }

        [TearDown]
        public void TearDown()
        {
            logger.Info("Start TearDown, runningThreads = {0}", runningThreads);
            foreach (var thread in threads ?? new List<Thread>())
            {
                if (thread.IsAlive)
                    thread.Abort();
            }
        }

        [TestCase(LocalRivalOptimization.Disabled, Category = "LongRunning")]
        [TestCase(LocalRivalOptimization.Enabled, Category = "LongRunning")]
        public void StressTest(LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(30, TimeSpan.FromSeconds(60), localRivalOptimization);
        }

        [TestCase(LocalRivalOptimization.Disabled)]
        [TestCase(LocalRivalOptimization.Enabled)]
        public void TestIncrementDecrementLock(LocalRivalOptimization localRivalOptimization)
        {
            DoTestIncrementDecrementLock(10, TimeSpan.FromSeconds(10), localRivalOptimization);
        }

        private void DoTestIncrementDecrementLock(int threadCount, TimeSpan runningTimeInterval, LocalRivalOptimization localRivalOptimization)
        {
            var remoteLockCreators = PrepareRemoteLockCreators(threadCount, localRivalOptimization, remoteLockImplementation);

            for (var i = 0; i < threadCount; i++)
                AddThread(IncrementDecrementAction, remoteLockCreators[i]);
            RunThreads(runningTimeInterval);
            JoinThreads();

            DisposeRemoteLockCreators(remoteLockCreators);
        }

        private void IncrementDecrementAction(IRemoteLockCreator lockCreator, Random random)
        {
            try
            {
                var remoteLock = lockCreator.Lock(lockId);
                using (remoteLock)
                {
                    logger.Info("MakeLock with threadId: " + remoteLock.ThreadId);
                    Thread.Sleep(1000);
                    CheckLocks(remoteLock.ThreadId);
                    Assert.AreEqual(0, ReadX());
                    logger.Info("Increment");
                    Interlocked.Increment(ref x);
                    logger.Info("Decrement");
                    Interlocked.Decrement(ref x);
                }
            }
            catch (Exception e)
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
            catch (Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        private void AddThread(Action<IRemoteLockCreator, Random> shortAction, IRemoteLockCreator lockCreator)
        {
            var seed = Guid.NewGuid().GetHashCode();
            var thread = new Thread(() => MakePeriodicAction(shortAction, seed, lockCreator));
            thread.Start();
            logger.Info("Add thread with seed = {0}", seed);
            threads.Add(thread);
        }

        private void JoinThreads()
        {
            logger.Info("JoinThreads. begin");
            isEnd = true;
            running.Set();
            var timeout = TimeSpan.FromMinutes(5);
            foreach (var thread in threads)
                Assert.That(thread.Join(timeout), $"Thread {thread.ManagedThreadId} didn't finish in {timeout}");
            logger.Info("JoinThreads. end");
        }

        private void RunThreads(TimeSpan runningTimeInterval)
        {
            logger.Info("RunThreads. begin, runningThreads = {0}", runningThreads);
            running.Set();
            Thread.Sleep(runningTimeInterval);
            running.Reset();
            while (Interlocked.CompareExchange(ref runningThreads, 0, 0) != 0)
            {
                Thread.Sleep(50);
                logger.Info("Wait runningThreads = 0. Now runningThreads = {0}", runningThreads);
                foreach (var thread in threads)
                {
                    if (!thread.IsAlive)
                        throw new Exception("Поток сдох");
                }
            }
            logger.Info("RunThreads. end");
        }

        private void MakePeriodicAction(Action<IRemoteLockCreator, Random> shortAction, int seed, IRemoteLockCreator lockCreator)
        {
            try
            {
                var localRandom = new Random(seed);
                while (!isEnd)
                {
                    running.WaitOne();
                    Interlocked.Increment(ref runningThreads);
                    shortAction(lockCreator, localRandom);
                    Interlocked.Decrement(ref runningThreads);
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static IRemoteLockCreator[] PrepareRemoteLockCreators(int threadCount, LocalRivalOptimization localRivalOptimization, CassandraRemoteLockImplementation remoteLockImplementation)
        {
            var remoteLockCreators = new IRemoteLockCreator[threadCount];
            var remoteLockerMetrics = new RemoteLockerMetrics(null);
            if (localRivalOptimization == LocalRivalOptimization.Enabled)
            {
                var singleRemoteLocker = new RemoteLocker(remoteLockImplementation, remoteLockerMetrics, logger);
                for (var i = 0; i < threadCount; i++)
                    remoteLockCreators[i] = singleRemoteLocker;
            }
            else
            {
                for (var i = 0; i < threadCount; i++)
                    remoteLockCreators[i] = new RemoteLocker(remoteLockImplementation, remoteLockerMetrics, logger);
            }
            return remoteLockCreators;
        }

        private static void DisposeRemoteLockCreators(IRemoteLockCreator[] remoteLockCreators)
        {
            foreach (var remoteLockCreator in remoteLockCreators)
                ((RemoteLocker)remoteLockCreator).Dispose();
        }

        private const string lockId = "IncDecLock";
        private int x;
        private CassandraRemoteLockImplementation remoteLockImplementation;
        private volatile bool isEnd;
        private int runningThreads;
        private List<Thread> threads;
        private readonly ManualResetEvent running = new ManualResetEvent(false);
        private static readonly ILog logger = Log4NetConfiguration.RootLogger.ForContext(nameof(RemoteLockTest));
    }
}