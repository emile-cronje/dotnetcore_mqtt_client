using System.Data;
using MqttTestClient.Models;
using Npgsql;
using NpgsqlTypes;

namespace MqttTestClient.DataAccess.Pg;

public class ToDoItemPgDao : PgDao<ToDoItem>, IToDoItemDao
{
    public ToDoItemPgDao(string tableName = "item") : base(tableName)
    {
    }

    public async Task<ToDoItem?> GetItem(long itemId)
    {
        var connection = GetConnection();
        ToDoItem? result = null;
        await using var cmd = new NpgsqlCommand(
            $"SELECT ID, VERSION, NAME, DESCRIPTION, IS_COMPLETE, CLIENT_ID, MESSAGE_ID FROM {_tableName} WHERE ID = {itemId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
            result = new ToDoItem(reader.GetInt32(5))
            {
                Id = reader.GetInt64(0),
                Version = reader.GetInt32(1),
                Name = reader.GetString(2),
                Description = reader.GetString(3),
                IsComplete = reader.GetBoolean(4),
                MessageId = reader.GetString(6)
            };

        await connection.CloseAsync();
        return result;
    }

    public void Initialise()
    {
        var connection = GetConnection();

        var command = $"DROP TABLE IF EXISTS {_tableName}";
        using var cmdDrop = new NpgsqlCommand(command, connection);
        cmdDrop.ExecuteNonQuery();

        command =
            $"CREATE TABLE IF NOT EXISTS {_tableName} (ID BIGINT PRIMARY KEY, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, NAME TEXT, DESCRIPTION TEXT, IS_COMPLETE BOOLEAN)";
        using var cmdCreate = new NpgsqlCommand(command, connection);

        cmdCreate.ExecuteNonQuery();
        
        command =
            $"CREATE UNIQUE INDEX index_{_tableName}_message_id ON {_tableName}(message_id)";
        using var cmdCreateIndex = new NpgsqlCommand(command, connection);
        
        cmdCreateIndex.ExecuteNonQuery();
        
        connection.Close();
    }

    public Task BatchInsert(List<ToDoItem> items)
    {
        lock (_batchLock)
        {
            if (items.Count == 0) return Task.CompletedTask;

            var connection = GetConnection();
            using var writer = connection.BeginBinaryImport(
                $"COPY {_tableName} (CLIENT_ID, ID, MESSAGE_ID, VERSION, NAME, DESCRIPTION, IS_COMPLETE) FROM STDIN (FORMAT BINARY)");

            foreach (var item in items)
            {
                writer.StartRow();
                writer.Write(item.ClientId);
                writer.Write(item.Id ?? 0);
                writer.Write(item.MessageId);
                writer.Write(item.Version);
                writer.Write(item.Name);
                writer.Write(item.Description);
                writer.Write(item.IsComplete);
            }

            writer.Complete();
            connection.Close();
        }

        return Task.CompletedTask;
    }

    public async Task BatchUpdate(List<ToDoItem> items, int batchSize)
    {
        if (items.Count == 0) return;

        var batches = CreateBatches(items, batchSize);

        foreach (var batch in batches)
            await PersistBatch(batch);
    }

    private async Task PersistBatch(List<ToDoItem> items)
    {
        await _flushSemaphore.WaitAsync();

        var maxRetries = 3;
        var retryDelayMilliseconds = 100;

        await using var connection = GetConnection();
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

        try
        {
            for (var retry = 0; retry < maxRetries; retry++)
                try
                {
                    var idsToUpdate = items.Select(i => i.Id).Where(id => id.HasValue).Select(id => id.Value).ToList();

                    if (!idsToUpdate.Any())
                    {
                        await transaction.CommitAsync();
                        return;
                    }

                    // First, get the existing IDs and lock them
                    var existingIds = new HashSet<long>();
                    
                    await using (var lockCmd = new NpgsqlCommand(
                        $"SELECT ID FROM {_tableName} WHERE ID = ANY(@remoteIds) FOR NO KEY UPDATE", 
                        connection, transaction))
                    {
                        lockCmd.Parameters.AddWithValue("remoteIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint, idsToUpdate);
                        await using var reader = await lockCmd.ExecuteReaderAsync();
                        
                        while (await reader.ReadAsync())
                        {
                            existingIds.Add(reader.GetInt64(0));
                        }
                    }

                    // Filter items to only include those that exist
                    var existingItems = items.Where(item => item.Id.HasValue && existingIds.Contains(item.Id.Value)).ToList();

                    if (!existingItems.Any())
                    {
                        await transaction.CommitAsync();
                        return; // No existing records to update
                    }

                    var tempTableName = $"temp_updates_{Guid.NewGuid():N}";
                    
                    await using (var cmd = new NpgsqlCommand(
                        $"CREATE TEMPORARY TABLE {tempTableName} (LIKE {_tableName} INCLUDING DEFAULTS)",
                        connection, transaction))
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }

                    // Only copy existing items to temp table
                    await using (var writer = await connection.BeginBinaryImportAsync(
                        $"COPY {tempTableName} (CLIENT_ID, ID, MESSAGE_ID, VERSION, NAME, DESCRIPTION, IS_COMPLETE) FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var item in existingItems)
                        {
                            await writer.StartRowAsync();
                            await writer.WriteAsync(item.ClientId);
                            await writer.WriteAsync(item.Id.Value);
                            await writer.WriteAsync(item.MessageId);
                            await writer.WriteAsync(item.Version);
                            await writer.WriteAsync(item.Name);
                            await writer.WriteAsync(item.Description);
                            await writer.WriteAsync(item.IsComplete);
                        }
                        
                        await writer.CompleteAsync();
                    }

                    int updatedCount;
                    
                    await using (var cmd = new NpgsqlCommand(
                        $@"UPDATE {_tableName} AS t
                            SET NAME = temp.NAME,
                                DESCRIPTION = temp.DESCRIPTION,
                                IS_COMPLETE = temp.IS_COMPLETE,
                                VERSION = temp.VERSION,
                                MESSAGE_ID = temp.MESSAGE_ID
                            FROM {tempTableName} AS temp
                            WHERE t.ID = temp.ID", 
                        connection, transaction))
                    {
                        updatedCount = await cmd.ExecuteNonQueryAsync();
                    }

                    if (updatedCount != existingItems.Count)
                    {
                        Console.WriteLine($"Warning: Expected to update {existingItems.Count} records but only updated {updatedCount}");
                    }

                    await transaction.CommitAsync();
                    return;
                }
                catch (PostgresException ex) when (ex.SqlState is "40P01" or "40001")
                {
                    await transaction.RollbackAsync();

                    if (retry < maxRetries - 1)
                    {
                        var delay = (int)(retryDelayMilliseconds * Math.Pow(2, retry) *
                                          (new Random().NextDouble() + 0.5));
                        await Task.Delay(delay);
                    }
                    else
                    {
                        throw new Exception(
                            "Transaction failed after multiple retries due to deadlock or serialization error.", ex);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("ToDoItem :" + e.Message);
                    await transaction.RollbackAsync();
                    throw;
                }
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }
    
    private async Task PersistBatchOld(List<ToDoItem> items)
    {
        await _flushSemaphore.WaitAsync();

        var maxRetries = 3;
        var retryDelayMilliseconds = 100;

        try
        {
            foreach (var item in items)
            {
                // Skip items without IDs
                if (!item.Id.HasValue)
                    continue;

                for (var retry = 0; retry < maxRetries; retry++)
                {
                    await using var connection = GetConnection();
                    await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

                    try
                    {
                        // First, check if the record exists and lock it
                        bool recordExists = false;
                        
                        await using (var checkCmd = new NpgsqlCommand(
                            $"SELECT 1 FROM {_tableName} WHERE ID = @id FOR NO KEY UPDATE", 
                            connection, transaction))
                        {
                            checkCmd.Parameters.AddWithValue("id", item.Id.Value);
                            var result = await checkCmd.ExecuteScalarAsync();
                            recordExists = result != null;
                        }

                        if (!recordExists)
                        {
                            await transaction.CommitAsync();
                            break; // Move to next item
                        }

                        // Update the existing record directly in the table
                        await using (var updateCmd = new NpgsqlCommand(
                            $@"UPDATE {_tableName} 
                               SET NAME = @name,
                                   DESCRIPTION = @description,
                                   IS_COMPLETE = @isComplete,
                                   VERSION = @version,
                                   MESSAGE_ID = @messageId
                               WHERE ID = @id", 
                            connection, transaction))
                        {
                            updateCmd.Parameters.AddWithValue("id", item.Id.Value);
                            updateCmd.Parameters.AddWithValue("name", item.Name);
                            updateCmd.Parameters.AddWithValue("description", item.Description);
                            updateCmd.Parameters.AddWithValue("isComplete", item.IsComplete);
                            updateCmd.Parameters.AddWithValue("version", item.Version);
                            updateCmd.Parameters.AddWithValue("messageId", item.MessageId);

                            var updatedRows = await updateCmd.ExecuteNonQueryAsync();
                            
                            if (updatedRows != 1)
                            {
                                Console.WriteLine($"Warning: Expected to update 1 record with ID {item.Id.Value} but updated {updatedRows}");
                            }
                        }

                        await transaction.CommitAsync();
                        break; // Success, move to next item
                    }
                    catch (PostgresException ex) when (ex.SqlState is "40P01" or "40001")
                    {
                        await transaction.RollbackAsync();

                        if (retry < maxRetries - 1)
                        {
                            var delay = (int)(retryDelayMilliseconds * Math.Pow(2, retry) *
                                              (new Random().NextDouble() + 0.5));
                            await Task.Delay(delay);
                        }
                        else
                        {
                            throw new Exception(
                                $"Transaction failed after multiple retries for item ID {item.Id.Value} due to deadlock or serialization error.", ex);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"ToDoItem ID {item.Id.Value}: {e.Message}");
                        await transaction.RollbackAsync();
                        throw;
                    }
                }
            }
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }    
}