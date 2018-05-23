﻿using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public static class TimeSpanExtensions
    {
        public static TimeSpan Multiply(this TimeSpan value, int factor)
        {
            return TimeSpan.FromTicks(value.Ticks * factor);
        }
    }
}