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
}