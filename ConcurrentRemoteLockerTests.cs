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
    [TestFixture(LocalRivalOptimization.Disabled, TimestampProviderStochasticType.BothPositiveAndNegative)]
    [TestFixture(LocalRivalOptimization.Disabled, TimestampProviderStochasticType.OnlyPositive)]
    [TestFixture(LocalRivalOptimization.Enabled, TimestampProviderStochasticType.BothPositiveAndNegative)]
    public class ConcurrentRemoteLockerTests
    {
        public ConcurrentRemoteLockerTests(LocalRivalOptimization localRivalOptimization, TimestampProviderStochasticType stochasticType)
        {
            this.localRivalOptimization = localRivalOptimization;
            this.stochasticType = stochasticType;
        }

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var cassandraCluster = new CassandraCluster(CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node));
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [TestCase(1, 1, 500, 0.01d, 0.2d, false)]
        [TestCase(2, 10, 100, 0.05d, 0.2d, false)]
        [TestCase(2, 10, 100, 0.05d, 0.2d, false)]
        [TestCase(5, 25, 100, 0.05d, 0.2d, false)]
        [TestCase(5, 25, 100, 0.05d, 0.2d, false)]
        [TestCase(10, 5, 500, 0.09d, 0.2d, false)]
        [TestCase(1, 10, 100, 0.09d, 0.2d, false)]
        [TestCase(1, 25, 100, 0.09d, 0.2d, false)]
        [TestCase(1, 10, 1000, 0.005d, 0.2d, false)]
        [TestCase(1, 25, 100, 0.00d, 1.00d, true)]
        [TestCase(1, 25, 1000, 0.00d, 1.00d, true)]
        [TestCase(1, 50, 50, 0.00d, 1.00d, true)]
        [TestCase(1, 50, 500, 0.00d, 1.00d, true)]
        [TestCase(1, 75, 750, 0.00d, 1.00d, true)]
        public void Lock(int locks, int threads, int operationsPerThread, double longRunningOpProbability, double fastRunningOpProbability, bool superfastKeepAlive)
        {
            var lockTtl = TimeSpan.FromSeconds(3);
            const int cassOpAttempts = 1;
            var cassOpTimeout = TimeSpan.FromSeconds(1);
            var config = new RemoteLockerTesterConfig
                {
                    LockCreatorsCount = threads,
                    LocalRivalOptimization = localRivalOptimization,
                    LockTtl = lockTtl,
                    KeepLockAliveInterval = superfastKeepAlive ? TimeSpan.Zero : TimeSpan.FromSeconds(1),
                    CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, cassOpAttempts, cassOpTimeout),
                    StochasticType = stochasticType
                };
            var lockIds = Enumerable.Range(0, locks).Select(x => Guid.NewGuid().ToString()).ToArray();
            var resources = new ConcurrentDictionary<string, Guid>();
            var opsCounters = new ConcurrentDictionary<string, int>();
            using(var tester = new RemoteLockerTester(config))
            {
                var stopSignal = new ManualResetEvent(false);
                var localTester = tester;
                var actions = new Action<MultithreadingTestHelper.RunState>[threads];
                for(var th = 0; th < actions.Length; th++)
                {
                    var remoteLockCreator = tester[th];
                    actions[th] = state =>
                        {
                            var rng = new Random(Guid.NewGuid().GetHashCode());
                            for(var op = 0; op < operationsPerThread; op++)
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
                                if(rng.NextDouble() < fastRunningOpProbability)
                                    opDuration = TimeSpan.Zero;
                                else if(rng.NextDouble() < longRunningOpProbability)
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
                Assert.That(opsCounters.Sum(x => x.Value), Is.EqualTo(threads * operationsPerThread));
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

        private readonly LocalRivalOptimization localRivalOptimization;
        private readonly TimestampProviderStochasticType stochasticType;
    }
}