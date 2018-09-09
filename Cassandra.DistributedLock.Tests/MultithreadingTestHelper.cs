using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

using NUnit.Framework;

namespace Cassandra.DistributedLock.Tests
{
    public static class MultithreadingTestHelper
    {
        private static Thread CreateThread(ConcurrentBag<Exception> errors, RunState runState, Action<RunState> action, string threadId = null)
        {
            return new Thread(() =>
                {
                    try
                    {
                        action(runState);
                    }
                    catch (Exception e)
                    {
                        errors.Add(e);
                        Console.WriteLine(e);
                    }
                })
                {
                    IsBackground = true,
                    Name = $"test-{threadId}",
                };
        }

        public static void RunOnSeparateThreads(TimeSpan timeout, params Action<RunState>[] actions)
        {
            var errors = new ConcurrentBag<Exception>();
            var runState = new RunState(errors);
            var threads = actions.Select((a, threadId) => CreateThread(errors, runState, a, threadId.ToString())).ToList();
            foreach (var t in threads)
                t.Start();
            foreach (var t in threads)
            {
                if (!t.Join(timeout))
                    Assert.Fail("Thread did not terminate in: {0}", timeout);
                Assert.That(errors, Is.Empty);
            }
        }

        public class RunState
        {
            public RunState(ConcurrentBag<Exception> exceptions)
            {
                this.exceptions = exceptions;
            }

            public bool ErrorOccurred => exceptions.Any();
            private readonly ConcurrentBag<Exception> exceptions;
        }
    }
}