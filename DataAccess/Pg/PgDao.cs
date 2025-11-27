using MqttTestClient.Models;
using Npgsql;

namespace MqttTestClient.DataAccess.Pg;

public class PgDao<T> : IPgDao where T : Entity
{
    protected readonly Lock _batchLock = new();
    protected readonly SemaphoreSlim _flushSemaphore = new(1, 1);
    protected readonly string _tableName;    

    public PgDao(string tableName)
    {
        _tableName = tableName;
    }
    
    protected NpgsqlConnection GetConnection()
    {
        var connectionString =
            "Host=127.0.0.1;Port = 5432;Database = pg_crud_test_client;Username = postgres;Password = 1793;SSL Mode=Disable;\n";
        var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        return connection;
    }

    public async Task BatchDelete(IEnumerable<long> ids)
    {
        await using var connection = GetConnection();

        // Using ANY and a parameter for efficiency and to prevent SQL injection
        await using var cmd = new NpgsqlCommand(
            $"DELETE FROM {_tableName} WHERE id = ANY(@ids)", connection);
        cmd.Parameters.AddWithValue("ids", ids.ToArray());

        await cmd.ExecuteNonQueryAsync();
    }

    protected List<List<T>> CreateBatches(List<T> allItems, int batchSize)
    {
        var highestMessageIdItems = allItems
            .GroupBy(item => item.Id)
            .Select(group => group.OrderByDescending(item => item.MessageId).First())
            .ToList();

        var batches = new List<List<T>>();
        var currentBatch = new List<T>();

        foreach (var item in highestMessageIdItems)
        {
            currentBatch.Add(item);

            if (currentBatch.Count != batchSize) continue;

            batches.Add(currentBatch);
            currentBatch = [];
        }

        if (currentBatch.Count > 0)
            batches.Add(currentBatch);

        return batches;
    }    

    public async Task<int> GetEntityVersion(long entityId)
    {
        var connection = GetConnection();
        var result = 0;
        await using var cmd = new NpgsqlCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {entityId}", connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync()) result = reader.GetInt32(0);

        await connection.CloseAsync();
        return result;
    }

    public async Task<int> GetEntityCount()
    {
        var connection = GetConnection();
        var result = 0;

        try
        {
            await using var cmd = new NpgsqlCommand(
                $"SELECT COUNT(CLIENT_ID) FROM {_tableName}", connection);

            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
                result = reader.GetInt32(0);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }

        await connection.CloseAsync();
        return result;
    }
}