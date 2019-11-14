using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.DistributedLock.Tests.FailedCassandra
{
    public class FailedColumnFamilyConnection : IColumnFamilyConnection
    {
        public FailedColumnFamilyConnection(IColumnFamilyConnection connection, double failProbability)
        {
            this.connection = connection;
            this.failProbability = failProbability;
        }

        public bool IsRowExist(string key)
        {
            MayBeFail();
            return connection.IsRowExist(key);
        }

        public void DeleteRows(string[] keys, long? timestamp = null, int batchSize = 1000)
        {
            MayBeFail();
            connection.DeleteRows(keys, timestamp, batchSize);
            MayBeFail();
        }

        public void DeleteRow(string key, long? timestamp = null)
        {
            MayBeFail();
            connection.DeleteRow(key, timestamp);
            MayBeFail();
        }

        public void DeleteColumn(string key, string columnName, long? timestamp = null)
        {
            MayBeFail();
            connection.DeleteColumn(key, columnName, timestamp);
            MayBeFail();
        }

        public void AddColumn(string key, Column column)
        {
            MayBeFail();
            connection.AddColumn(key, column);
            MayBeFail();
        }

        public void AddColumn(Func<int, KeyColumnPair<string>> createKeyColumnPair)
        {
            MayBeFail();
            connection.AddColumn(createKeyColumnPair);
            MayBeFail();
        }

        public Column GetColumn(string key, string columnName)
        {
            MayBeFail();
            return connection.GetColumn(key, columnName);
        }

        public bool TryGetColumn(string key, string columnName, out Column result)
        {
            MayBeFail();
            return connection.TryGetColumn(key, columnName, out result);
        }

        public void DeleteBatch(string key, IEnumerable<string> columnNames, long? timestamp = null)
        {
            MayBeFail();
            connection.DeleteBatch(key, columnNames, timestamp);
            MayBeFail();
        }

        public void AddBatch(string key, IEnumerable<Column> columns)
        {
            MayBeFail();
            connection.AddBatch(key, columns);
            MayBeFail();
        }

        public void AddBatch(Func<int, KeyColumnsPair<string>> createKeyColumnsPair)
        {
            MayBeFail();
            connection.AddBatch(createKeyColumnsPair);
            MayBeFail();
        }

        public void BatchInsert(IEnumerable<KeyValuePair<string, IEnumerable<Column>>> data)
        {
            MayBeFail();
            connection.BatchInsert(data);
            MayBeFail();
        }

        public void BatchDelete(IEnumerable<KeyValuePair<string, IEnumerable<string>>> data, long? timestamp = null)
        {
            MayBeFail();
            connection.BatchDelete(data, timestamp);
            MayBeFail();
        }

        public List<KeyValuePair<string, Column[]>> GetRegion(IEnumerable<string> keys, string startColumnName, string finishColumnName, int limitPerRow)
        {
            MayBeFail();
            return connection.GetRegion(keys, startColumnName, finishColumnName, limitPerRow);
        }

        public List<KeyValuePair<string, Column[]>> GetRowsExclusive(IEnumerable<string> keys, string exclusiveStartColumnName, int count)
        {
            MayBeFail();
            return connection.GetRowsExclusive(keys, exclusiveStartColumnName, count);
        }

        public List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string[] columnNames)
        {
            MayBeFail();
            return connection.GetRows(keys, columnNames);
        }

        public void Truncate()
        {
            connection.Truncate();
        }

        public Column[] GetColumns(string key, string exclusiveStartColumnName, int count, bool reversed = false)
        {
            MayBeFail();
            return connection.GetColumns(key, exclusiveStartColumnName, count, reversed);
        }

        public Column[] GetColumns(string key, string startColumnName, string endColumnName, int count, bool reversed = false)
        {
            MayBeFail();
            return connection.GetColumns(key, startColumnName, endColumnName, count, reversed);
        }

        public Column[] GetColumns(string key, string[] columnNames)
        {
            MayBeFail();
            return connection.GetColumns(key, columnNames);
        }

        public IEnumerable<Column> GetRow(string key, int batchSize = 1000)
        {
            MayBeFail();
            return connection.GetRow(key, batchSize);
        }

        public IEnumerable<Column> GetRow(string key, string exclusiveStartColumnName, int batchSize = 1000)
        {
            MayBeFail();
            return connection.GetRow(key, exclusiveStartColumnName, batchSize);
        }

        public string[] GetKeys(string exclusiveStartKey, int count)
        {
            MayBeFail();
            return connection.GetKeys(exclusiveStartKey, count);
        }

        public IEnumerable<string> GetKeys(int batchSize = 1000)
        {
            MayBeFail();
            return connection.GetKeys(batchSize);
        }

        public int GetCount(string key)
        {
            MayBeFail();
            return connection.GetCount(key);
        }

        public Dictionary<string, int> GetCounts(IEnumerable<string> keys)
        {
            MayBeFail();
            return connection.GetCounts(keys);
        }

        public ICassandraConnectionParameters GetConnectionParameters()
        {
            return connection.GetConnectionParameters();
        }

        private void MayBeFail()
        {
            if (ThreadLocalRandom.Instance.NextDouble() < failProbability)
                throw new FailedCassandraClusterException("Ошибка при работе с кассандрой");
        }

        private readonly IColumnFamilyConnection connection;
        private readonly double failProbability;
    }
}