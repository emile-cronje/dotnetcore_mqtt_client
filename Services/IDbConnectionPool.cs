using Microsoft.Data.Sqlite;

namespace MqttTestClient.Services;

public interface IDbConnectionPool
{
    SqliteConnection GetConnection();
    void ReturnConnection(SqliteConnection connection);
}