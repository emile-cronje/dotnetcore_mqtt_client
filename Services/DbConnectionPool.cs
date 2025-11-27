using Microsoft.Data.Sqlite;
using System.Collections.Concurrent;

namespace MqttTestClient.Services;

public class DbConnectionPool : IDbConnectionPool
{
    private readonly ConcurrentBag<SqliteConnection> _connections;
    private readonly string _connectionString;
    private readonly int _maxSize;
    private int _currentSize;

    public DbConnectionPool(string connectionString, int maxSize = 200)
    {
        _connections = new ConcurrentBag<SqliteConnection>();
        _connectionString = connectionString;
        _maxSize = maxSize;
        _currentSize = 0;
    }

    public SqliteConnection GetConnection()
    {
        if (_connections.TryTake(out var connection)) return connection;

        if (_currentSize < _maxSize)
        {
            Interlocked.Increment(ref _currentSize);
            return new SqliteConnection(_connectionString);
        }

        throw new InvalidOperationException("No available connections.");
    }

    public void ReturnConnection(SqliteConnection connection)
    {
        _connections.Add(connection);
    }
}