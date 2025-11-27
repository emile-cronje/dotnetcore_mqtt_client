using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class ToDoItemSqliteDao : SqliteDao, IToDoItemDao
{
    private readonly string _tableName;

    public ToDoItemSqliteDao(IDbConnectionPool connectionPool) : base(connectionPool)
    {
        _tableName = "todo_item";
    }

    public async Task<ToDoItem?> GetItem(long itemId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"SELECT id, version, name, description, is_complete, client_id, message_id FROM {_tableName} WHERE id = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", itemId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localTodoItem = new ToDoItem(reader.GetInt32(5))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            Name = reader.GetString(2),
            Description = reader.GetString(3),
            IsComplete = reader.GetBoolean(4),
            MessageId = reader.GetString(5)
        };

        return localTodoItem;
    }

    public async Task BatchInsert(List<ToDoItem> items)
    {
        foreach (var item in items) await AddItem(item.ClientId.ToString(), item);
    }

    public async Task BatchDelete(IEnumerable<long> ids)
    {
        foreach (var id in ids) await DeleteItem(id);
    }

    public async Task BatchUpdate(List<ToDoItem> items, int batchSize)
    {
        foreach (var item in items) await UpdateItem(item);
    }

    public void Initialise()
    {
        var connection = GetConnection();

        var command = $"DROP TABLE IF EXISTS {_tableName}";
        var cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        command =
            $"CREATE TABLE {_tableName} (ID INTEGER PRIMARY KEY, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID INTEGER, NAME TEXT, DESCRIPTION TEXT, IS_COMPLETE INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        ReturnConnection(connection);
    }

    public Task<int> GetEntityVersion(long entityId)
    {
        throw new NotImplementedException();
    }

    public async Task<int> GetEntityCount()
    {
        var itemCount = 0;

        var connection = GetConnection();
        await using var cmdCount =
            new SqliteCommand($"SELECT count(*) from {_tableName}", connection);
        await using var readerCount = await cmdCount.ExecuteReaderAsync();

        if (readerCount.Read())
            itemCount = readerCount.GetInt32(0);

        ReturnConnection(connection);
        return itemCount;
    }

    public async Task UpdateItem(ToDoItem toDoItem)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = {toDoItem.Version}, NAME = '{toDoItem.Name}', DESCRIPTION = '{toDoItem.Description}', IS_COMPLETE = {toDoItem.IsComplete}, MESSAGE_ID = {toDoItem.MessageId} WHERE ID = '{toDoItem.Id}'",
            connection);

        await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);
    }

    public async Task DeleteItem(long itemId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE ID = '{itemId}'",
            connection);

        await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();
    }

    public async Task<int> GetItemVersion(long itemId)
    {
        var dbVersion = -1;

        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"SELECT version from {_tableName} where id = '{itemId}'",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (reader.Read()) dbVersion = reader.GetInt32(0);
        ReturnConnection(connection);

        return dbVersion;
    }

    public async Task<int> GetItemCount()
    {
        var itemCount = 0;

        var connection = GetConnection();
        await using var cmdCount =
            new SqliteCommand($"SELECT count(*) from {_tableName}", connection);
        await using var readerCount = await cmdCount.ExecuteReaderAsync();

        if (readerCount.Read())
            itemCount = readerCount.GetInt32(0);

        ReturnConnection(connection);
        return itemCount;
    }

    public async Task AddItem(string clientId, ToDoItem toDoItem)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (CLIENT_ID, ID, MESSAGE_ID, VERSION, NAME, DESCRIPTION, IS_COMPLETE) VALUES ('{clientId}', '{toDoItem.Id}', {toDoItem.MessageId}, {toDoItem.Version}, '{toDoItem.Name}', '{toDoItem.Description}', {toDoItem.IsComplete})",
            connection);

        try
        {
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }

        ReturnConnection(connection);
    }
}