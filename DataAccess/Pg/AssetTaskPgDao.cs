using System.Data;
using MqttTestClient.Models;
using Npgsql;
using NpgsqlTypes;

namespace MqttTestClient.DataAccess.Pg;

public class AssetTaskPgDao : PgDao<AssetTask>, IAssetTaskDao
{
    public AssetTaskPgDao(string tableName = "asset_task") : base(tableName)
    {
    }

    public async Task<AssetTask?> GetAssetTask(long assetTaskId)
    {
        var connection = GetConnection();
        AssetTask? result = null;
        await using var cmd = new NpgsqlCommand(
            $"SELECT ID, VERSION, ASSET_ID, CODE, DESCRIPTION, IS_RFS, CLIENT_ID, MESSAGE_ID FROM {_tableName} WHERE ID = {assetTaskId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
            result = new AssetTask(reader.GetInt32(6))
            {
                Id = reader.GetInt64(0),
                Version = reader.GetInt32(1),
                AssetId = reader.GetInt64(2),
                Code = reader.GetString(3),
                Description = reader.GetString(4),
                IsRfs = reader.GetBoolean(5),
                MessageId = reader.GetString(7)
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
            $"CREATE TABLE IF NOT EXISTS {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, ASSET_ID BIGINT, CODE TEXT, DESCRIPTION TEXT, IS_RFS BOOLEAN)";
        using var cmdCreate = new NpgsqlCommand(command, connection);

        cmdCreate.ExecuteNonQuery();
        
        command =
            $"CREATE UNIQUE INDEX index_{_tableName}_message_id ON {_tableName}(message_id)";
        using var cmdCreateIndex = new NpgsqlCommand(command, connection);
        
        cmdCreateIndex.ExecuteNonQuery();
        
        connection.Close();
    }

    public Task BatchInsert(List<AssetTask> assetTasks)
    {
        lock (_batchLock)
        {
            if (assetTasks.Count == 0) return Task.CompletedTask;

            var connection = GetConnection();
            using var writer = connection.BeginBinaryImport(
                $"COPY {_tableName} (CLIENT_ID, ID, ASSET_ID, MESSAGE_ID, VERSION, CODE, DESCRIPTION, IS_RFS) FROM STDIN (FORMAT BINARY)");

            foreach (var assetTask in assetTasks)
            {
                writer.StartRow();
                writer.Write(assetTask.ClientId);
                writer.Write(assetTask.Id ?? 0);
                writer.Write(assetTask.AssetId);
                writer.Write(assetTask.MessageId);
                writer.Write(assetTask.Version);
                writer.Write(assetTask.Code);
                writer.Write(assetTask.Description);
                writer.Write(assetTask.IsRfs);
            }

            writer.Complete();
            connection.Close();
        }

        return Task.CompletedTask;
    }

    public async Task BatchUpdate(List<AssetTask> assetTasks, int batchSize)
    {
        if (assetTasks.Count == 0) return;

        var batches = CreateBatches(assetTasks, batchSize);

        foreach (var batch in batches)
            await PersistBatch(batch);
    }

    private async Task PersistBatch(List<AssetTask> assetTasks)
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
                    var idsToUpdate = assetTasks.Select(i => i.Id).Where(id => id.HasValue).Select(id => id.Value).ToList();

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
                    var existingItems = assetTasks.Where(item => item.Id.HasValue && existingIds.Contains(item.Id.Value)).ToList();

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

                    await using (var writer = await connection.BeginBinaryImportAsync(
                                     $"COPY {tempTableName} (CLIENT_ID, ID, ASSET_ID, MESSAGE_ID, VERSION, CODE, DESCRIPTION, IS_RFS) FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var assetTask in assetTasks)
                        {
                            await writer.StartRowAsync();
                            await writer.WriteAsync(assetTask.ClientId);
                            await writer.WriteAsync(assetTask.Id ?? 0);
                            await writer.WriteAsync(assetTask.AssetId);
                            await writer.WriteAsync(assetTask.MessageId);
                            await writer.WriteAsync(assetTask.Version);
                            await writer.WriteAsync(assetTask.Code);
                            await writer.WriteAsync(assetTask.Description);
                            await writer.WriteAsync(assetTask.IsRfs);
                        }

                        await writer.CompleteAsync();
                    }

                    int updatedCount;                    
                    await using (var cmd = new NpgsqlCommand(
                                     $@"UPDATE {_tableName} AS t
                            SET CODE = temp.CODE,
                                DESCRIPTION = temp.DESCRIPTION,
                                IS_RFS = temp.IS_RFS,
                                VERSION = temp.VERSION,
                                MESSAGE_ID = temp.MESSAGE_ID
                            FROM {tempTableName} AS temp
                            WHERE t.ID = temp.ID", connection,
                                     transaction))
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
                    Console.WriteLine("Asset Task :" + e.Message);
                    await transaction.RollbackAsync();
                    throw;
                }
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }
    
    private async Task PersistBatchOld(List<AssetTask> assetTasks)
    {
        await _flushSemaphore.WaitAsync();

        var maxRetries = 3;
        var retryDelayMilliseconds = 100;

        try
        {
            foreach (var assetTask in assetTasks)
            {
                // Skip items without IDs
                if (!assetTask.Id.HasValue)
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
                            checkCmd.Parameters.AddWithValue("id", assetTask.Id.Value);
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
                               SET CODE = @code,
                                   DESCRIPTION = @description,
                                   IS_RFS = @isRfs,                                   
                                   VERSION = @version,
                                   MESSAGE_ID = @messageId
                               WHERE ID = @id", 
                            connection, transaction))
                        {
                            updateCmd.Parameters.AddWithValue("id", assetTask.Id.Value);
                            updateCmd.Parameters.AddWithValue("code", assetTask.Code);
                            updateCmd.Parameters.AddWithValue("description", assetTask.Description);
                            updateCmd.Parameters.AddWithValue("isRfs", assetTask.IsRfs);                            
                            updateCmd.Parameters.AddWithValue("version", assetTask.Version);
                            updateCmd.Parameters.AddWithValue("messageId", assetTask.MessageId);

                            var updatedRows = await updateCmd.ExecuteNonQueryAsync();
                            
                            if (updatedRows != 1)
                            {
                                Console.WriteLine($"Warning: Expected to update 1 record with ID {assetTask.Id.Value} but updated {updatedRows}");
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
                                $"Transaction failed after multiple retries for item ID {assetTask.Id.Value} due to deadlock or serialization error.", ex);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"ToDoItem ID {assetTask.Id.Value}: {e.Message}");
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