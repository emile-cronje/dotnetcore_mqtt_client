using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class ToDoItemSqliteDao : SqliteDao, IToDoItemDao
{
    private readonly string _tableName;

    public ToDoItemSqliteDao(IDbConnectionPool connectionPool, string tableName = "todo_item") : base(connectionPool)
    {
        _tableName = tableName;
    }

    public async Task<ToDoItem?> GetItem(long itemId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"SELECT ID, VERSION, MESSAGE_ID, NAME, DESCRIPTION, IS_COMPLETE, CLIENT_ID FROM {_tableName} WHERE ID = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", itemId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localTodoItem = new ToDoItem(reader.GetInt32(6))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            MessageId = reader.GetString(2),
            Name = reader.GetString(3),
            Description = reader.GetString(4),
            IsComplete = reader.GetBoolean(5)
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
            $"CREATE TABLE {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, NAME TEXT, DESCRIPTION TEXT, IS_COMPLETE INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        command = $"CREATE UNIQUE INDEX idx_{_tableName}_message_id ON {_tableName}(MESSAGE_ID)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        InitializeWalMode(connection);

        ReturnConnection(connection);
    }

    public async Task<int> GetEntityVersion(long entityId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {entityId}",
            connection);

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!reader.HasRows)
            {
                ReturnConnection(connection);
                return -1;
            }

            reader.Read();
            var version = reader.GetInt32(0);
            ReturnConnection(connection);
            return version;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    public async Task<int> GetEntityCount()
    {
        var itemCount = 0;

        var connection = GetConnection();
        await using var cmdCount =
            new SqliteCommand($"SELECT COUNT(*) FROM {_tableName}", connection);
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
            $"UPDATE {_tableName} SET VERSION = {toDoItem.Version}, MESSAGE_ID = '{toDoItem.MessageId}', NAME = '{toDoItem.Name}', DESCRIPTION = '{toDoItem.Description}', IS_COMPLETE = {toDoItem.IsComplete} WHERE ID = {toDoItem.Id}",
            connection);

        await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);
    }

    public async Task UpdateItemOld(ToDoItem toDoItem)
    {
        var connection = GetConnection();
        
        // Fetch the current version from the database
        await using var versionCmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = @ID",
            connection);
        versionCmd.Parameters.AddWithValue("@ID", toDoItem.Id);
        
        await using var versionReader = await versionCmd.ExecuteReaderAsync();
        if (!versionReader.Read())
        {
            ReturnConnection(connection);
            throw new InvalidOperationException($"ToDoItem ID {toDoItem.Id} not found");
        }
        
        var currentVersion = versionReader.GetInt32(0);
        
        // Now update using the current version from the database
        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = @NEW_VERSION, MESSAGE_ID = @MESSAGE_ID, NAME = @NAME, DESCRIPTION = @DESCRIPTION, IS_COMPLETE = @IS_COMPLETE WHERE ID = @ID AND VERSION = @VERSION",
            connection);

        cmd.Parameters.AddWithValue("@ID", toDoItem.Id);
        cmd.Parameters.AddWithValue("@VERSION", currentVersion);
        cmd.Parameters.AddWithValue("@NEW_VERSION", currentVersion + 1);        
        cmd.Parameters.AddWithValue("@MESSAGE_ID", toDoItem.MessageId);
        cmd.Parameters.AddWithValue("@NAME", toDoItem.Name);
        cmd.Parameters.AddWithValue("@DESCRIPTION", toDoItem.Description);
        cmd.Parameters.AddWithValue("@IS_COMPLETE", toDoItem.IsComplete);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);
        
        if (rowsAffected == 0)
        {
            throw new InvalidOperationException($"Version mismatch for ToDoItem ID {toDoItem.Id}. Current DB version {currentVersion}, in-memory version {toDoItem.Version}");
        }
    }

    public async Task DeleteItem(long itemId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE ID = {itemId}",
            connection);

        await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();
    }

    public async Task<int> GetItemVersion(long itemId)
    {
        var dbVersion = -1;

        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {itemId}",
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
            new SqliteCommand($"SELECT COUNT(*) FROM {_tableName}", connection);
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
            $"INSERT INTO {_tableName} (ID, VERSION, CLIENT_ID, MESSAGE_ID, NAME, DESCRIPTION, IS_COMPLETE) VALUES ({toDoItem.Id}, {toDoItem.Version}, {clientId}, '{toDoItem.MessageId}', '{toDoItem.Name}', '{toDoItem.Description}', {toDoItem.IsComplete})",
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