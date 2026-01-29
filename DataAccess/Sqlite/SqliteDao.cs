using Microsoft.Data.Sqlite;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class SqliteDao
{
    private readonly IDbConnectionPool _connectionPool;

    public SqliteDao(IDbConnectionPool connectionPool)
    {
        _connectionPool = connectionPool;
    }

    protected SqliteConnection GetConnection()
    {
        SqliteConnection connection = _connectionPool.GetConnection();
        connection.Open();
        return connection;
    }

    protected void ReturnConnection(SqliteConnection connection)
    {
        _connectionPool.ReturnConnection(connection);
    }

    protected void InitializeWalMode(SqliteConnection connection)
    {
        // Enable WAL mode for better concurrency
        using var cmd = new SqliteCommand("PRAGMA journal_mode = WAL;", connection);
        cmd.ExecuteNonQuery();

        // Set busy timeout to 5 seconds for automatic retries on lock conflicts
        cmd.CommandText = "PRAGMA busy_timeout = 5000;";
        cmd.ExecuteNonQuery();
    }
}