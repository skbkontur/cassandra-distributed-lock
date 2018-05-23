using System;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.Commons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

using Vostok.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [TestFixture]
    public class BasicRemoteLockerTests
    {
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var cassandraCluster = new CassandraCluster(SingleCassandraNodeSetUpFixture.Node.CreateSettings(), logger);
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), new CassandraInitializerSettings());
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [Test]
        public void TryLock_SingleLockId()
        {
            using(var tester = new RemoteLockerTester())
            {
                var lockId = Guid.NewGuid().ToString();
                IRemoteLock lock1, lock2;
                Assert.That(tester.TryGetLock(lockId, out lock1), Is.True);
                Assert.That(lock1, Is.Not.Null);
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.False);
                Assert.That(lock2, Is.Null);
                lock1.Dispose();
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.True);
                Assert.That(lock2, Is.Not.Null);
                lock2.Dispose();
            }
        }

        [Test]
        public void TryLock_DifferentLockIds()
        {
            using(var tester = new RemoteLockerTester())
            {
                var lockId1 = Guid.NewGuid().ToString();
                var lockId2 = Guid.NewGuid().ToString();
                var lockId3 = Guid.NewGuid().ToString();
                IRemoteLock lock1, lock2, lock3;
                Assert.That(tester.TryGetLock(lockId1, out lock1), Is.True);
                Assert.That(tester.TryGetLock(lockId2, out lock2), Is.True);
                Assert.That(tester.TryGetLock(lockId3, out lock3), Is.True);
                lock1.Dispose();
                lock2.Dispose();
                lock3.Dispose();
            }
        }

        [Test]
        public void Lock()
        {
            using(var tester = new RemoteLockerTester())
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester.Lock(lockId);
                IRemoteLock lock2;
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.False);
                lock1.Dispose();
                Assert.That(tester.TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
            }
        }

        [Test]
        public void LockIsKeptAlive_Success()
        {
            var config = new RemoteLockerTesterConfig
                {
                    LockersCount = 2,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled,
                    LockTtl = TimeSpan.FromSeconds(10),
                    LockMetadataTtl = TimeSpan.FromMinutes(1),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(5),
                    ChangeLockRowThreshold = 10,
                    TimestamProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraClusterSettings = SingleCassandraNodeSetUpFixture.Node.CreateSettings(attempts : 1, timeout : TimeSpan.FromSeconds(1)),
                };
            using(var tester = new RemoteLockerTester(config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester[0].Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(12)); // waiting in total: 12 = 1*1 + 10 + 1 sec
                IRemoteLock lock2;
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.False);
                lock1.Dispose();
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
            }
        }

        [Test]
        public void LockIsKeptAlive_Failure()
        {
            var config = new RemoteLockerTesterConfig
                {
                    LockersCount = 2,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled,
                    LockTtl = TimeSpan.FromSeconds(5),
                    LockMetadataTtl = TimeSpan.FromMinutes(1),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(10),
                    ChangeLockRowThreshold = 10,
                    TimestamProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraClusterSettings = SingleCassandraNodeSetUpFixture.Node.CreateSettings(attempts : 1, timeout : TimeSpan.FromSeconds(1)),
                };
            using(var tester = new RemoteLockerTester(config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester[0].Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(3));
                IRemoteLock lock2;
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.False);
                Thread.Sleep(TimeSpan.FromSeconds(4)); // waiting in total: 3 + 4 = 1*1 + 5 + 1 sec
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
                lock1.Dispose();
            }
        }

        private static readonly ILog logger = new Log4NetWrapper(typeof(BasicRemoteLockerTests));
    }
}