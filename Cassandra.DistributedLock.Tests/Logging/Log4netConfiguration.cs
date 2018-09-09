using log4net.Config;

namespace Cassandra.DistributedLock.Tests.Logging
{
    public static class Log4NetConfiguration
    {
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