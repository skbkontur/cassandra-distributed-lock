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
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [TestCase(1, 1, 500, 0.01, LocalRivalOptimization.Disabled, false)]
        [TestCase(2, 10, 100, 0.05, LocalRivalOptimization.Enabled, false)]
        [TestCase(2, 10, 100, 0.05, LocalRivalOptimization.Disabled, false)]
        [TestCase(5, 25, 100, 0.05, LocalRivalOptimization.Enabled, false)]
        [TestCase(5, 25, 100, 0.05, LocalRivalOptimization.Disabled, false)]
        [TestCase(10, 5, 500, 0.09, LocalRivalOptimization.Disabled, false)]
        [TestCase(1, 10, 1000, 0.005, LocalRivalOptimization.Disabled, true)]
        [TestCase(1, 10, 1000, 0.005, LocalRivalOptimization.Disabled, false)]
        public void Lock(int locks, int threads, int operationsPerThread, double longRunningOpProbability, LocalRivalOptimization localRivalOptimization, bool enableSyncer)
        {
            const double fastRunningOpProbability = 0.20;
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
            var previousThresholds = Enumerable.Range(0, locks).Select(i => (long?)0L).ToArray();
            var opsCounter = 0;
            var resources = new ConcurrentDictionary<string, Guid>();
            using(var tester = new RemoteLockerTester(config))
            {
                var stopSignal = new ManualResetEvent(false);
                var syncSignal = new ManualResetEvent(true);
                Task syncerThread = null;
                if(enableSyncer)
                {
                    syncerThread = Task.Factory.StartNew(() =>
                        {
                            do
                            {
                                syncSignal.Reset();
                                Thread.Sleep(TimeSpan.FromMilliseconds(300));
                                syncSignal.Set();
                            } while(!stopSignal.WaitOne(TimeSpan.FromSeconds(3)));
                        });
                }
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
                                syncSignal.WaitOne();
                                var @lock = Lock(remoteLockCreator, rng, lockId, state);
                                if(@lock == null)
                                    break;
                                if(++opsCounter % (threads * operationsPerThread / 100) == 0)
                                    Console.Out.Write(".");
                                var resource = Guid.NewGuid();
                                resources[lockId] = resource;
                                var opDuration = TimeSpan.FromMilliseconds(16);
                                if(rng.NextDouble() < fastRunningOpProbability)
                                    opDuration = TimeSpan.Zero;
                                else if(rng.NextDouble() < longRunningOpProbability)
                                    opDuration = opDuration.Add(lockTtl).Add(cassOpTimeout.Multiply(cassOpAttempts));
                                Thread.Sleep(opDuration);
                                Assert.That(resources[lockId], Is.EqualTo(resource));
                                CollectionAssert.AreEqual(new[] {@lock.ThreadId}, localTester.GetThreadsInMainRow(lockId));
                                Assert.That(localTester.GetThreadsInShadeRow(lockId), Is.Not.Contains(@lock.ThreadId));
                                var lockMetadata = localTester.GetLockMetadata(lockId);
                                Assert.That(lockMetadata.PreviousThreshold, Is.GreaterThan(previousThresholds[lockIndex]));
                                previousThresholds[lockIndex] = lockMetadata.PreviousThreshold;
                                Assert.That(lockMetadata.ProbableOwnerThreadId, Is.EqualTo(@lock.ThreadId));
                                @lock.Dispose();
                                Assert.That(localTester.GetThreadsInMainRow(lockId), Is.Not.Contains(@lock.ThreadId));
                            }
                        };
                }
                MultithreadingTestHelper.RunOnSeparateThreads(TimeSpan.FromMinutes(30), actions);
                stopSignal.Set();
                if(syncerThread != null)
                    syncerThread.Wait();
                Assert.That(opsCounter, Is.EqualTo(threads * operationsPerThread));
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