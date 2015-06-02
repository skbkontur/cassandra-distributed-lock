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
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.RemoteLockTests
{
    [TestFixture]
    public abstract class RemoteLockTestBase
    {
        [TestFixtureSetUp]
        public virtual void TestFixtureSetUp()
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
            var remoteLockImplementation = container.Create<CassandraRemoteLockImplementationSettings, CassandraRemoteLockImplementation>(new CassandraRemoteLockImplementationSettings
                {
                    ColumnFamilyFullName = ColumnFamilies.remoteLock,
                    LockTtl = TimeSpan.FromMinutes(3),
                    KeepLockAliveInterval = TimeSpan.FromSeconds(15),
                });
            container.Configurator.ForAbstraction<IRemoteLockImplementation>().UseInstances(remoteLockImplementation);

            logger.InfoFormat("Start SetUp, runningThreads = {0}", runningThreads);
            runningThreads = 0;
            isEnd = false;
            threads = new List<Thread>();
        }

        [TearDown]
        public virtual void TearDown()
        {
            logger.InfoFormat("Start TeadDown, runningThreads = {0}", runningThreads);
            foreach(var thread in threads ?? new List<Thread>())
                thread.Abort();
        }

        protected virtual void ConfigureContainer(IContainer c)
        {
        }

        protected void AddThread(Action<RemoteLockCreator, Random> shortAction, RemoteLockCreator lockCreator)
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

        private void MakePeriodicAction(Action<RemoteLockCreator, Random> shortAction, int seed, RemoteLockCreator lockCreator)
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

        protected static RemoteLockCreator[] PrepareRemoteLockCreators(int threadCount, bool localRivalOptimization, CassandraRemoteLockImplementation remoteLockImplementation, out RemoteLockLocalManager[] remoteLockLocalManagers)
        {
            remoteLockLocalManagers = new RemoteLockLocalManager[threadCount];
            var remoteLockCreators = new RemoteLockCreator[threadCount];
            if(localRivalOptimization)
            {
                var singleManager = new RemoteLockLocalManager(remoteLockImplementation);
                var singleLockCreator = new RemoteLockCreator(singleManager);
                for(var i = 0; i < threadCount; i++)
                {
                    remoteLockLocalManagers[i] = singleManager;
                    remoteLockCreators[i] = singleLockCreator;
                }
            }
            else
            {
                for(var i = 0; i < threadCount; i++)
                {
                    var manager = new RemoteLockLocalManager(remoteLockImplementation);
                    remoteLockLocalManagers[i] = manager;
                    remoteLockCreators[i] = new RemoteLockCreator(manager);
                }
            }
            return remoteLockCreators;
        }

        protected Container container;
        private volatile bool isEnd;
        private int runningThreads;
        private List<Thread> threads;
        private readonly ManualResetEvent running = new ManualResetEvent(false);
        private static readonly ILog logger = LogManager.GetLogger(typeof(RemoteLockTestBase));
    }
}