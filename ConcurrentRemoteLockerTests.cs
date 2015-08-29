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

        [TestCase(true, 1, 1, 500, 0.01, LocalRivalOptimization.Disabled)]
        [TestCase(true, 2, 10, 100, 0.05, LocalRivalOptimization.Enabled)]
        [TestCase(true, 2, 10, 100, 0.05, LocalRivalOptimization.Disabled)]
        [TestCase(true, 5, 25, 100, 0.05, LocalRivalOptimization.Enabled)]
        [TestCase(true, 5, 25, 100, 0.05, LocalRivalOptimization.Disabled)]
        [TestCase(true, 10, 5, 500, 0.09, LocalRivalOptimization.Disabled)]
        [TestCase(false, 1, 1, 500, 0.01, LocalRivalOptimization.Disabled)]
        [TestCase(false, 2, 10, 100, 0.05, LocalRivalOptimization.Enabled)]
        [TestCase(false, 2, 10, 100, 0.05, LocalRivalOptimization.Disabled)]
        [TestCase(false, 5, 25, 100, 0.05, LocalRivalOptimization.Enabled)]
        [TestCase(false, 5, 25, 100, 0.05, LocalRivalOptimization.Disabled)]
        [TestCase(false, 10, 5, 500, 0.09, LocalRivalOptimization.Disabled)]
        public void Lock(bool useSingleLockKeeperThread, int locks, int threads, int operationsPerThread, double longRunningOpProbability, LocalRivalOptimization localRivalOptimization)
        {
            var lockTtl = TimeSpan.FromSeconds(3);
            const int cassOpAttempts = 1;
            var cassOpTimeout = TimeSpan.FromSeconds(1);
            var config = new RemoteLockerTesterConfig
                {
                    LockCreatorsCount = threads,
                    LocalRivalOptimization = localRivalOptimization,
                    LockTtl = lockTtl,
                    KeepLockAliveInterval = TimeSpan.FromSeconds(1),
                    CassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, cassOpAttempts, cassOpTimeout),
                };
            var lockIds = Enumerable.Range(0, locks).Select(x => Guid.NewGuid().ToString()).ToArray();
            var resources = new ConcurrentDictionary<string, Guid>();
            using(var tester = new RemoteLockerTester(useSingleLockKeeperThread, config))
            {
                var localTester = tester;
                var actions = new Action<MultithreadingTestHelper.RunState>[threads];
                for(var th = 0; th < actions.Length; th++)
                {
                    var remoteLockCreator = tester[th];
                    actions[th] = (state) =>
                        {
                            var rng = new Random(Guid.NewGuid().GetHashCode());
                            long? previousThreshold = 0L;
                            for(var op = 0; op < operationsPerThread; op++)
                            {
                                if(state.ErrorOccurred)
                                    break;
                                var lockId = lockIds[rng.Next(lockIds.Length)];
                                var @lock = Lock(remoteLockCreator, rng, lockId, state);
                                if(state.ErrorOccurred)
                                    break;
                                var resource = Guid.NewGuid();
                                resources[lockId] = resource;
                                var opDuration = TimeSpan.FromMilliseconds(16);
                                if(rng.NextDouble() < longRunningOpProbability)
                                    opDuration = opDuration.Add(lockTtl).Add(cassOpTimeout.Multiply(cassOpAttempts));
                                Thread.Sleep(opDuration);
                                Assert.That(resources[lockId], Is.EqualTo(resource));
                                CollectionAssert.AreEqual(new[] { @lock.ThreadId }, localTester.GetThreadsInMainRow(lockId));
                                CollectionAssert.IsEmpty(localTester.GetThreadsInShadeRow(lockId));
                                var threshold = localTester.GetThreshold(lockId);
                                Assert.That(threshold, Is.Not.Null);
                                Assert.That(threshold, Is.GreaterThan(previousThreshold));
                                previousThreshold = threshold;
                                @lock.Dispose();
                                Assert.That(localTester.GetThreadsInMainRow(lockId), Is.Not.Contains(@lock.ThreadId));
                            }
                        };
                }
                MultithreadingTestHelper.RunOnSeparateThreads(TimeSpan.FromMinutes(30), actions);
            }
        }

        private static IRemoteLock Lock(IRemoteLockCreator remoteLockCreator, Random rng, string lockId, MultithreadingTestHelper.RunState state)
        {
            IRemoteLock remoteLock;
            while(!remoteLockCreator.TryGetLock(lockId, out remoteLock) && !state.ErrorOccurred)
                Thread.Sleep(rng.Next(32));
            return remoteLock;
        }
    }
}