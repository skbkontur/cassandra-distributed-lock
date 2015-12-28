using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
            cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [SetUp]
        public void SetUp()
        {
            cassandraSchemeActualizer.TruncateAllColumnFamilies();
        }

        [TestCase(1, 1, 500, 0.01d, LocalRivalOptimization.Disabled, null)]
        [TestCase(2, 10, 100, 0.05d, LocalRivalOptimization.Enabled, null)]
        [TestCase(2, 10, 100, 0.05d, LocalRivalOptimization.Disabled, null)]
        [TestCase(5, 25, 100, 0.05d, LocalRivalOptimization.Enabled, null)]
        [TestCase(5, 25, 100, 0.05d, LocalRivalOptimization.Disabled, null)]
        [TestCase(10, 5, 500, 0.09d, LocalRivalOptimization.Disabled, null)]
        [TestCase(1, 25, 100, 0.09d, LocalRivalOptimization.Enabled, null)]
        [TestCase(1, 25, 100, 0.09d, LocalRivalOptimization.Disabled, null)]
        [TestCase(1, 10, 1000, 0.005d, LocalRivalOptimization.Disabled, null)]
        [TestCase(1, 10, 1000, 0.005d, LocalRivalOptimization.Disabled, 10)]
        [TestCase(1, 5, 100, 0.3d, LocalRivalOptimization.Enabled, null)]
        [TestCase(1, 5, 100, 0.3d, LocalRivalOptimization.Disabled, null)]
        public void Normal(int locks, int threads, int operationsPerThread, double longRunningOpProbability, LocalRivalOptimization localRivalOptimization, int? syncIntervalInSeconds)
        {
            DoTest(new TestConfig
                {
                    Locks = locks,
                    LongRunningOpProbability = longRunningOpProbability,
                    OperationsPerThread = operationsPerThread,
                    FastRunningOpProbability = 0.2d,
                    SyncInterval = syncIntervalInSeconds.HasValue ? TimeSpan.FromSeconds(syncIntervalInSeconds.Value) : (TimeSpan?)null,
                    TesterConfig = new RemoteLockerTesterConfig
                        {
                            LockersCount = threads,
                            LocalRivalOptimization = localRivalOptimization,
                            LockTtl = TimeSpan.FromSeconds(3),
                            KeepLockAliveInterval = TimeSpan.FromSeconds(1),
                            ChangeLockRowThreshold = 10,
                            TimestamProviderStochasticType = TimestampProviderStochasticType.None,
                            CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, 1, TimeSpan.FromSeconds(1)),
                        },
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
                    OperationsPerThread = operationsPerThread,
                    FastRunningOpProbability = 0.90d,
                    LongRunningOpProbability = 0.00d,
                    SyncInterval = null,
                    TesterConfig = new RemoteLockerTesterConfig
                        {
                            LockersCount = threads,
                            LocalRivalOptimization = LocalRivalOptimization.Disabled,
                            LockTtl = TimeSpan.FromMinutes(3),
                            KeepLockAliveInterval = TimeSpan.Zero,
                            ChangeLockRowThreshold = 2,
                            TimestamProviderStochasticType = stochasticType,
                            CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, 1, TimeSpan.FromSeconds(1)),
                        },
                });
        }

        [TestCase(1, 1, 1000, TimestampProviderStochasticType.None)]
        [TestCase(1, 10, 1000, TimestampProviderStochasticType.None)]
        [TestCase(1, 25, 1000, TimestampProviderStochasticType.OnlyPositive)]
        [TestCase(1, 25, 1000, TimestampProviderStochasticType.BothPositiveAndNegative)]
        public void SmallTtl(int locks, int threads, int operationsPerThread, TimestampProviderStochasticType stochasticType)
        {
            DoTest(new TestConfig
                {
                    Locks = locks,
                    OperationsPerThread = operationsPerThread,
                    FastRunningOpProbability = 1.00d,
                    LongRunningOpProbability = 0.00d,
                    SyncInterval = null,
                    TesterConfig = new RemoteLockerTesterConfig
                        {
                            LockersCount = threads,
                            LocalRivalOptimization = LocalRivalOptimization.Disabled,
                            LockTtl = TimeSpan.FromMilliseconds(2000),
                            KeepLockAliveInterval = TimeSpan.FromMilliseconds(50),
                            ChangeLockRowThreshold = int.MaxValue,
                            TimestamProviderStochasticType = stochasticType,
                            CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, 1, TimeSpan.FromMilliseconds(350)),
                        },
                });
        }

        private static void DoTest(TestConfig cfg)
        {
            var cassandraOpTimeout = TimeSpan.FromMilliseconds(cfg.TesterConfig.CassandraClusterSettings.Timeout);
            var longOpDuration = cfg.TesterConfig.LockTtl.Add(cassandraOpTimeout).Multiply(cfg.TesterConfig.CassandraClusterSettings.Attempts);
            var lockIds = Enumerable.Range(0, cfg.Locks).Select(x => Guid.NewGuid().ToString()).ToArray();
            var resources = new ConcurrentDictionary<string, Guid>();
            var opsCounters = new ConcurrentDictionary<string, int>();
            using(var tester = new RemoteLockerTester(cfg.TesterConfig))
            {
                var stopSignal = new ManualResetEvent(false);
                var syncSignal = new ManualResetEvent(true);
                Task syncerThread = null;
                if(cfg.SyncInterval.HasValue)
                {
                    syncerThread = Task.Factory.StartNew(() =>
                        {
                            do
                            {
                                syncSignal.Reset();
                                Thread.Sleep(longOpDuration);
                                syncSignal.Set();
                            } while(!stopSignal.WaitOne(cfg.SyncInterval.Value));
                        });
                }
                var localTester = tester;
                var actions = new Action<MultithreadingTestHelper.RunState>[cfg.TesterConfig.LockersCount];
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
                                var @lock = Lock(remoteLockCreator, syncSignal, rng, lockId, state);
                                if(@lock == null)
                                    break;
                                var localOpsCounter = opsCounters.GetOrAdd(lockId, 0);
                                var resource = Guid.NewGuid();
                                resources[lockId] = resource;
                                var opDuration = TimeSpan.FromMilliseconds(rng.Next(1, 47));
                                if(rng.NextDouble() < cfg.FastRunningOpProbability)
                                    opDuration = TimeSpan.Zero;
                                else if(rng.NextDouble() < cfg.LongRunningOpProbability)
                                    opDuration = opDuration.Add(longOpDuration);
                                Thread.Sleep(opDuration);
                                CollectionAssert.AreEqual(new[] {@lock.ThreadId}, localTester.GetThreadsInMainRow(lockId));
                                Assert.That(localTester.GetThreadsInShadeRow(lockId), Is.Not.Contains(@lock.ThreadId));
                                var lockMetadata = localTester.GetLockMetadata(lockId);
                                Assert.That(lockMetadata.ProbableOwnerThreadId, Is.EqualTo(@lock.ThreadId));
                                Assert.That(resources[lockId], Is.EqualTo(resource));
                                Assert.That(opsCounters[lockId], Is.EqualTo(localOpsCounter));
                                if(++localOpsCounter % (cfg.TesterConfig.LockersCount * cfg.OperationsPerThread / 100) == 0)
                                    Console.Out.Write(".");
                                opsCounters[lockId] = localOpsCounter;
                                @lock.Dispose();
                                Thread.Sleep(1);
                                Assert.That(localTester.GetThreadsInMainRow(lockId), Is.Not.Contains(@lock.ThreadId));
                            }
                        };
                }
                MultithreadingTestHelper.RunOnSeparateThreads(TimeSpan.FromMinutes(30), actions);
                stopSignal.Set();
                if(syncerThread != null)
                    syncerThread.Wait();
                Assert.That(opsCounters.Sum(x => x.Value), Is.EqualTo(cfg.TesterConfig.LockersCount * cfg.OperationsPerThread));
            }
        }

        private static IRemoteLock Lock(IRemoteLockCreator remoteLockCreator, ManualResetEvent syncSignal, Random rng, string lockId, MultithreadingTestHelper.RunState state)
        {
            while(true)
            {
                if(state.ErrorOccurred)
                    return null;
                syncSignal.WaitOne();
                IRemoteLock remoteLock;
                if(remoteLockCreator.TryGetLock(lockId, out remoteLock))
                    return remoteLock;
                Thread.Sleep(rng.Next(32));
            }
        }

        private CassandraSchemeActualizer cassandraSchemeActualizer;

        private class TestConfig
        {
            public int Locks { get; set; }
            public int OperationsPerThread { get; set; }
            public double LongRunningOpProbability { get; set; }
            public double FastRunningOpProbability { get; set; }
            public TimeSpan? SyncInterval { get; set; }
            public RemoteLockerTesterConfig TesterConfig { get; set; }
        }
    }
}