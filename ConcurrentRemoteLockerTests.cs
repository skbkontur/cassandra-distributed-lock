using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [TestFixture]
    public class ConcurrentRemoteLockerTests
    {
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var cassandraCluster = new CassandraCluster(CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node));
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [TestCase(1, 1, 500, 0.01d, LocalRivalOptimization.Disabled)]
        [TestCase(2, 10, 100, 0.05d, LocalRivalOptimization.Enabled)]
        [TestCase(2, 10, 100, 0.05d, LocalRivalOptimization.Disabled)]
        [TestCase(5, 25, 100, 0.05d, LocalRivalOptimization.Enabled)]
        [TestCase(5, 25, 100, 0.05d, LocalRivalOptimization.Disabled)]
        [TestCase(10, 5, 500, 0.09d, LocalRivalOptimization.Disabled)]
        [TestCase(1, 25, 100, 0.09d, LocalRivalOptimization.Enabled)]
        [TestCase(1, 25, 100, 0.09d, LocalRivalOptimization.Disabled)]
        [TestCase(1, 10, 1000, 0.005d, LocalRivalOptimization.Disabled)]
        [TestCase(1, 5, 100, 0.3d, LocalRivalOptimization.Enabled)]
        [TestCase(1, 5, 100, 0.3d, LocalRivalOptimization.Disabled)]
        public void Normal(int locks, int threads, int operationsPerThread, double longRunningOpProbability, LocalRivalOptimization localRivalOptimization)
        {
            DoTest(new TestConfig
                {
                    Locks = locks,
                    Threads = threads,
                    LongRunningOpProbability = longRunningOpProbability,
                    LocalRivalOptimization = localRivalOptimization,
                    OperationsPerThread = operationsPerThread,
                    FastRunningOpProbability = 0.2d,
                    KeepLockAliveInterval = TimeSpan.FromSeconds(1),
                    TimestampProviderStochasticType = TimestampProviderStochasticType.None
                });
        }

        [TestCase(1, 37, 1000, TimestampProviderStochasticType.BothPositiveAndNegative)]
        [TestCase(1, 37, 1000, TimestampProviderStochasticType.OnlyPositive)]
        [TestCase(1, 50, 750, TimestampProviderStochasticType.BothPositiveAndNegative)]
        [TestCase(1, 50, 750, TimestampProviderStochasticType.OnlyPositive)]
        public void HighFrequencyKeepAlive(int locks, int threads, int operationsPerThread, TimestampProviderStochasticType stochasticType)
        {
            DoTest(new TestConfig
                {
                    Locks = locks,
                    Threads = threads,
                    OperationsPerThread = operationsPerThread,
                    TimestampProviderStochasticType = stochasticType,
                    FastRunningOpProbability = 1.00d,
                    LongRunningOpProbability = 0.00d,
                    KeepLockAliveInterval = TimeSpan.Zero,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled
                });
        }

        private static void DoTest(TestConfig cfg)
        {
            var lockTtl = TimeSpan.FromSeconds(3);
            const int cassOpAttempts = 1;
            var cassOpTimeout = TimeSpan.FromSeconds(1);
            var config = new RemoteLockerTesterConfig
                {
                    LockCreatorsCount = cfg.Threads,
                    LocalRivalOptimization = cfg.LocalRivalOptimization,
                    LockTtl = lockTtl,
                    KeepLockAliveInterval = cfg.KeepLockAliveInterval,
                    CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, cassOpAttempts, cassOpTimeout),
                    StochasticType = cfg.TimestampProviderStochasticType
                };
            var lockIds = Enumerable.Range(0, cfg.Locks).Select(x => Guid.NewGuid().ToString()).ToArray();
            var resources = new ConcurrentDictionary<string, Guid>();
            var opsCounters = new ConcurrentDictionary<string, int>();
            using(var tester = new RemoteLockerTester(config))
            {
                var stopSignal = new ManualResetEvent(false);
                var localTester = tester;
                var actions = new Action<MultithreadingTestHelper.RunState>[cfg.Threads];
                for(var th = 0; th < actions.Length; th++)
                {
                    var remoteLockCreator = tester[th];
                    actions[th] = state =>
                        {
                            var rng = new Random(Guid.NewGuid().GetHashCode());
                            for(var op = 0; op < cfg.OperationsPerThread; op++)
                            {
                                if(state.ErrorOccurred)
                                    break;
                                var lockIndex = rng.Next(lockIds.Length);
                                var lockId = lockIds[lockIndex];
                                var @lock = Lock(remoteLockCreator, rng, lockId, state);
                                if(@lock == null)
                                    break;
                                var localOpsCounter = opsCounters.GetOrAdd(lockId, 0);
                                var resource = Guid.NewGuid();
                                resources[lockId] = resource;
                                var opDuration = TimeSpan.FromMilliseconds(rng.Next(1, 47));
                                if(rng.NextDouble() < cfg.FastRunningOpProbability)
                                    opDuration = TimeSpan.Zero;
                                else if(rng.NextDouble() < cfg.LongRunningOpProbability)
                                    opDuration = opDuration.Add(lockTtl).Add(cassOpTimeout.Multiply(cassOpAttempts));
                                Thread.Sleep(opDuration);
                                CollectionAssert.AreEqual(new[] {@lock.ThreadId}, localTester.GetThreadsInMainRow(lockId));
                                Assert.That(localTester.GetThreadsInShadeRow(lockId), Is.Not.Contains(@lock.ThreadId));
                                var lockMetadata = localTester.GetLockMetadata(lockId);
                                Assert.That(lockMetadata.ProbableOwnerThreadId, Is.EqualTo(@lock.ThreadId));
                                Assert.That(resources[lockId], Is.EqualTo(resource));
                                Assert.That(opsCounters[lockId], Is.EqualTo(localOpsCounter));
                                opsCounters[lockId] = localOpsCounter + 1;
                                @lock.Dispose();
                                Thread.Sleep(1);
                                Assert.That(localTester.GetThreadsInMainRow(lockId), Is.Not.Contains(@lock.ThreadId));
                            }
                        };
                }
                MultithreadingTestHelper.RunOnSeparateThreads(TimeSpan.FromMinutes(30), actions);
                stopSignal.Set();
                Assert.That(opsCounters.Sum(x => x.Value), Is.EqualTo(cfg.Threads * cfg.OperationsPerThread));
            }
        }

        private static IRemoteLock Lock(IRemoteLockCreator remoteLockCreator, Random rng, string lockId, MultithreadingTestHelper.RunState state)
        {
            while(true)
            {
                for(var i = 0; i < 10; i++)
                {
                    if(state.ErrorOccurred)
                        break;
                    IRemoteLock remoteLock;
                    if(remoteLockCreator.TryGetLock(lockId, out remoteLock))
                        return remoteLock;
                }
                if(state.ErrorOccurred)
                    break;
                Thread.Sleep(1);
            }
            return null;
        }

        private class TestConfig
        {
            public int Locks { get; set; }
            public int Threads { get; set; }
            public int OperationsPerThread { get; set; }
            public double LongRunningOpProbability { get; set; }
            public double FastRunningOpProbability { get; set; }
            public LocalRivalOptimization LocalRivalOptimization { get; set; }
            public TimestampProviderStochasticType TimestampProviderStochasticType { get; set; }
            public TimeSpan KeepLockAliveInterval { get; set; }
        }
    }
}