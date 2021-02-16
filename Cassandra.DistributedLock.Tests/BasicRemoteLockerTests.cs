using System;
using System.Threading;

using NUnit.Framework;

namespace Cassandra.DistributedLock.Tests
{
    public class BasicRemoteLockerTests
    {
        [Test]
        public void TryLock_SingleLockId()
        {
            using (var tester = new RemoteLockerTester())
            {
                var lockId = Guid.NewGuid().ToString();
                Assert.That(tester.TryGetLock(lockId, out var lock1), Is.True);
                Assert.That(lock1, Is.Not.Null);
                Assert.That(tester.TryGetLock(lockId, out var lock2), Is.False);
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
            using (var tester = new RemoteLockerTester())
            {
                var lockId1 = Guid.NewGuid().ToString();
                var lockId2 = Guid.NewGuid().ToString();
                var lockId3 = Guid.NewGuid().ToString();
                Assert.That(tester.TryGetLock(lockId1, out var lock1), Is.True);
                Assert.That(tester.TryGetLock(lockId2, out var lock2), Is.True);
                Assert.That(tester.TryGetLock(lockId3, out var lock3), Is.True);
                lock1.Dispose();
                lock2.Dispose();
                lock3.Dispose();
            }
        }

        [Test]
        public void Lock()
        {
            using (var tester = new RemoteLockerTester())
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester.Lock(lockId);
                Assert.That(tester.TryGetLock(lockId, out var lock2), Is.False);
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
                    KeepLockAliveInterval = TimeSpan.FromSeconds(4),
                    ChangeLockRowThreshold = 10,
                    TimestampProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraClusterSettings = SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(attempts : 1, timeout : TimeSpan.FromSeconds(1)),
                };
            using (var tester = new RemoteLockerTester(config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester[0].Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(12)); // waiting in total: 12 = 1*1 + 10 + 1 sec
                Assert.That(tester[1].TryGetLock(lockId, out var lock2), Is.False);
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
                    TimestampProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraClusterSettings = SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(attempts : 1, timeout : TimeSpan.FromSeconds(1)),
                };
            using (var tester = new RemoteLockerTester(config))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = tester[0].Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(3));
                Assert.That(tester[1].TryGetLock(lockId, out var lock2), Is.False);
                Thread.Sleep(TimeSpan.FromSeconds(4)); // waiting in total: 3 + 4 = 1*1 + 5 + 1 sec
                Assert.That(tester[1].TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
                lock1.Dispose();
            }
        }

        [Test]
        public void LockCancellationToken_IsCancelled_AfterLosingLock()
        {
            var config = new RemoteLockerTesterConfig
                {
                    LockersCount = 1,
                    LocalRivalOptimization = LocalRivalOptimization.Disabled,
                    LockTtl = TimeSpan.FromSeconds(5),
                    LockMetadataTtl = TimeSpan.FromMinutes(1),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(3),
                    ChangeLockRowThreshold = 10,
                    TimestampProviderStochasticType = TimestampProviderStochasticType.None,
                    CassandraClusterSettings = SingleCassandraNodeSetUpFixture.CreateCassandraClusterSettings(attempts : 1, timeout : TimeSpan.FromSeconds(1)),
                };

            using (var tester = new RemoteLockerTester(config))
            {
                var lockId = Guid.NewGuid().ToString();
                using (var lock1 = tester[0].Lock(lockId))
                {
                    Assert.That(lock1.LockAliveToken.IsCancellationRequested, Is.False);

                    // waiting more than LockTtl * 0.5 with no lock prolongation
                    Thread.Sleep(config.KeepLockAliveInterval);

                    Assert.That(lock1.LockAliveToken.IsCancellationRequested, Is.True);
                }
            }
        }
    }
}