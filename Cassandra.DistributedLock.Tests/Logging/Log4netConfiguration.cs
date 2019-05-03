using System.Linq;

using log4net;
using log4net.Config;

using Vostok.Logging.Log4net;

using ILog = Vostok.Logging.Abstractions.ILog;

namespace Cassandra.DistributedLock.Tests.Logging
{
    public static class Log4NetConfiguration
    {
        public static ILog RootLogger { get; } = new Log4netLog(LogManager.GetLogger(string.Empty))
            {
                LoggerNameFactory = ctx => string.Join(".", ctx.Reverse())
            };

        public static void InitializeOnce()
        {
            if (!initialized)
            {
                var type = typeof(Log4NetConfiguration);
                XmlConfigurator.Configure(type.Assembly.GetManifestResourceStream(type, "log4net.config"));
                initialized = true;
            }
        }

        private static bool initialized;
    }
}