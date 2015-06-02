using System;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [TestFixture]
    public class BasicRemoteLockTests
    {
        [Test]
        public void TryLock_SingleLockId()
        {
            using(var remoteLockTester = new RemoteLockTester())
            {
                var lockId = Guid.NewGuid().ToString();
                IRemoteLock lock1, lock2;
                Assert.That(remoteLockTester.TryGetLock(lockId, out lock1), Is.True);
                Assert.That(lock1, Is.Not.Null);
                Assert.That(remoteLockTester.TryGetLock(lockId, out lock2), Is.False);
                Assert.That(lock2, Is.Null);
                lock1.Dispose();
                Assert.That(remoteLockTester.TryGetLock(lockId, out lock2), Is.True);
                Assert.That(lock2, Is.Not.Null);
                lock2.Dispose();
            }
        }

        [Test]
        public void TryLock_DifferentLockIds()
        {
            using(var remoteLockTester = new RemoteLockTester())
            {
                var lockId1 = Guid.NewGuid().ToString();
                var lockId2 = Guid.NewGuid().ToString();
                var lockId3 = Guid.NewGuid().ToString();
                IRemoteLock lock1, lock2, lock3;
                Assert.That(remoteLockTester.TryGetLock(lockId1, out lock1), Is.True);
                Assert.That(remoteLockTester.TryGetLock(lockId2, out lock2), Is.True);
                Assert.That(remoteLockTester.TryGetLock(lockId3, out lock3), Is.True);
                lock1.Dispose();
                lock2.Dispose();
                lock3.Dispose();
            }
        }

        [Test]
        public void Lock()
        {
            using(var remoteLockTester = new RemoteLockTester())
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = remoteLockTester.Lock(lockId);
                IRemoteLock lock2;
                Assert.That(remoteLockTester.TryGetLock(lockId, out lock2), Is.False);
                lock1.Dispose();
                Assert.That(remoteLockTester.TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
            }
        }

        [Test]
        public void LockIsKeptAlive_Success()
        {
            var cassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, attempts: 1, timeout: TimeSpan.FromSeconds(1));
            using (var remoteLockTester1 = new RemoteLockTester(cassandraClusterSettings, lockTtl: TimeSpan.FromSeconds(10), keepLockAliveInterval: TimeSpan.FromSeconds(5)))
            using (var remoteLockTester2 = new RemoteLockTester(cassandraClusterSettings, lockTtl: TimeSpan.FromSeconds(10), keepLockAliveInterval: TimeSpan.FromSeconds(5)))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = remoteLockTester1.Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(12)); // waiting in total: 12 = 1*1 + 10 + 1 sec
                IRemoteLock lock2;
                Assert.That(remoteLockTester2.TryGetLock(lockId, out lock2), Is.False);
                lock1.Dispose();
                Assert.That(remoteLockTester2.TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
            }
        }

        [Test]
        public void LockIsKeptAlive_Failure()
        {
            var cassandraClusterSettings = CassandraClusterSettings.ForNode(StartSingleCassandraSetUp.Node, attempts : 1, timeout : TimeSpan.FromSeconds(1));
            using(var remoteLockTester1 = new RemoteLockTester(cassandraClusterSettings, lockTtl : TimeSpan.FromSeconds(5), keepLockAliveInterval : TimeSpan.FromSeconds(10)))
            using(var remoteLockTester2 = new RemoteLockTester(cassandraClusterSettings, lockTtl : TimeSpan.FromSeconds(5), keepLockAliveInterval : TimeSpan.FromSeconds(10)))
            {
                var lockId = Guid.NewGuid().ToString();
                var lock1 = remoteLockTester1.Lock(lockId);
                Thread.Sleep(TimeSpan.FromSeconds(3));
                IRemoteLock lock2;
                Assert.That(remoteLockTester2.TryGetLock(lockId, out lock2), Is.False);
                Thread.Sleep(TimeSpan.FromSeconds(4)); // waiting in total: 3 + 4 = 1*1 + 5 + 1 sec
                Assert.That(remoteLockTester2.TryGetLock(lockId, out lock2), Is.True);
                lock2.Dispose();
                lock1.Dispose();
            }
        }
    }
}