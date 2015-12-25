using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using GroboContainer.Core;
using GroboContainer.Impl;

using GroBuf;
using GroBuf.DataMembersExtracters;

using log4net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [TestFixture]
    public abstract class RemoteLockTestBase
    {
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var cassandraClusterSettings = StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback);
            var initializerSettings = new CassandraInitializerSettings();
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings), new CassandraMetaProvider(), initializerSettings);
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [SetUp]
        public virtual void SetUp()
        {
            var assemblies = AssembliesLoader.Load();
            container = new Container(new ContainerConfiguration(assemblies));
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead));
            ConfigureContainer(container);
            var cassandraClusterSettings = StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback);
            container.Configurator.ForAbstraction<ICassandraClusterSettings>().UseInstances(cassandraClusterSettings);
            var settings = new CassandraRemoteLockImplementationSettings(new DefaultTimestampProvider(), ColumnFamilies.remoteLock, TimeSpan.FromMinutes(3), TimeSpan.FromSeconds(5), 10);
            var remoteLockImplementation = container.Create<CassandraRemoteLockImplementationSettings, CassandraRemoteLockImplementation>(settings);
            container.Configurator.ForAbstraction<IRemoteLockImplementation>().UseInstances(remoteLockImplementation);

            logger.InfoFormat("Start SetUp, runningThreads = {0}", runningThreads);
            runningThreads = 0;
            isEnd = false;
            threads = new List<Thread>();
        }

        [TearDown]
        public void TearDown()
        {
            logger.InfoFormat("Start TeadDown, runningThreads = {0}", runningThreads);
            foreach(var thread in threads ?? new List<Thread>())
                thread.Abort();
        }

        protected virtual void ConfigureContainer(IContainer c)
        {
        }

        protected void AddThread(Action<IRemoteLockCreator, Random> shortAction, IRemoteLockCreator lockCreator)
        {
            var seed = Guid.NewGuid().GetHashCode();
            var thread = new Thread(() => MakePeriodicAction(shortAction, seed, lockCreator));
            thread.Start();
            logger.InfoFormat("Add thread with seed = {0}", seed);
            threads.Add(thread);
        }

        protected void JoinThreads()
        {
            logger.Info("JoinThreads. begin");
            isEnd = true;
            running.Set();
            foreach(var thread in threads)
                Assert.That(thread.Join(TimeSpan.FromSeconds(180)), "Не удалось остановить поток");
            logger.Info("JoinThreads. end");
        }

        protected void RunThreads(TimeSpan runningTimeInterval)
        {
            logger.InfoFormat("RunThreads. begin, runningThreads = {0}", runningThreads);
            running.Set();
            Thread.Sleep(runningTimeInterval);
            running.Reset();
            while(Interlocked.CompareExchange(ref runningThreads, 0, 0) != 0)
            {
                Thread.Sleep(50);
                logger.InfoFormat("Wait runningThreads = 0. Now runningThreads = {0}", runningThreads);
                foreach(var thread in threads)
                {
                    if(!thread.IsAlive)
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
                while(!isEnd)
                {
                    running.WaitOne();
                    Interlocked.Increment(ref runningThreads);
                    shortAction(lockCreator, localRandom);
                    Interlocked.Decrement(ref runningThreads);
                }
            }
            catch(Exception e)
            {
                logger.Error(e);
            }
        }

        protected static IRemoteLockCreator[] PrepareRemoteLockCreators(int threadCount, LocalRivalOptimization localRivalOptimization, CassandraRemoteLockImplementation remoteLockImplementation)
        {
            var remoteLockCreators = new IRemoteLockCreator[threadCount];
            var remoteLockerMetrics = new RemoteLockerMetrics(null);
            if(localRivalOptimization == LocalRivalOptimization.Enabled)
            {
                var singleRemoteLocker = new RemoteLocker(remoteLockImplementation, remoteLockerMetrics);
                for(var i = 0; i < threadCount; i++)
                    remoteLockCreators[i] = singleRemoteLocker;
            }
            else
            {
                for(var i = 0; i < threadCount; i++)
                    remoteLockCreators[i] = new RemoteLocker(remoteLockImplementation, remoteLockerMetrics);
            }
            return remoteLockCreators;
        }

        protected static void DisposeRemoteLockCreators(IRemoteLockCreator[] remoteLockCreators)
        {
            foreach(var remoteLockCreator in remoteLockCreators)
                ((RemoteLocker)remoteLockCreator).Dispose();
        }

        protected static void CheckLockIsNotAcquiredLocally(IRemoteLockCreator[] remoteLockCreators, string lockId)
        {
            //проверяем, что после всего мы в какой-то момент сможем-таки взять лок
            foreach(var remoteLockCreator in remoteLockCreators)
                Assert.That(!((RemoteLocker)remoteLockCreator).CheckLockIsAcquiredLocally(lockId), "После остановки всех потоков осталась локальная блокировка");
        }

        protected Container container;
        private volatile bool isEnd;
        private int runningThreads;
        private List<Thread> threads;
        private readonly ManualResetEvent running = new ManualResetEvent(false);
        private static readonly ILog logger = LogManager.GetLogger(typeof(RemoteLockTestBase));
    }
}