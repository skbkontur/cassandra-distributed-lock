using System;

using Metrics;
using Metrics.Utils;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker
{
    public static class MetricsExtensions
    {
        public static IDisposable NewContext(this Timer timer, Action<TimeSpan> finalAction, string userValue = null)
        {
            return new TimeMeasuringContext(timer, finalAction, userValue);
        }

        private struct TimeMeasuringContext : IDisposable
        {
            private readonly Timer timer;
            private readonly Action<TimeSpan> finalAction;
            private readonly string userValue;

            private readonly long start;
            private bool disposed;

            public TimeMeasuringContext(Timer timer, Action<TimeSpan> finalAction, string userValue = null)
            {
                this.timer = timer;
                this.finalAction = finalAction;
                this.userValue = userValue;

                start = timer.StartRecording();
                disposed = false;
            }

            public void Dispose()
            {
                if(!disposed)
                {
                    disposed = true;
                    var elapsed = timer.EndRecording() - start;
                    timer.Record(elapsed, TimeUnit.Nanoseconds, userValue);
                    finalAction(TimeSpan.FromMilliseconds(TimeUnit.Nanoseconds.ToMilliseconds(elapsed)));
                }
            }
        }
    }
}