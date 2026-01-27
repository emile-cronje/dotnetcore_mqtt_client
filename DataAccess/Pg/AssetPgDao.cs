using System.Data;
using MqttTestClient.Models;
using Npgsql;
using NpgsqlTypes;

namespace MqttTestClient.DataAccess.Pg;

public class AssetPgDao : PgDao<Asset>, IAssetDao
{
    public AssetPgDao(string tableName = "asset") : base(tableName)
    {
    }

    public async Task<Asset?> GetAsset(long assetId)
    {
        var connection = GetConnection();
        Asset? result = null;
        await using var cmd = new NpgsqlCommand(
            $"SELECT ID, VERSION, CODE, DESCRIPTION, IS_MSI, CLIENT_ID, MESSAGE_ID FROM {_tableName} WHERE ID = {assetId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
            result = new Asset(reader.GetInt32(5))
            {
                Id = reader.GetInt64(0),
                Version = reader.GetInt32(1),
                Code = reader.GetString(2),
                Description = reader.GetString(3),
                IsMsi = reader.GetBoolean(4),
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
            $"CREATE TABLE IF NOT EXISTS {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, CODE TEXT, DESCRIPTION TEXT, IS_MSI BOOLEAN)";
        using var cmdCreate = new NpgsqlCommand(command, connection);

        cmdCreate.ExecuteNonQuery();
        
        command =
            $"CREATE UNIQUE INDEX index_{_tableName}_message_id ON {_tableName}(message_id)";
        using var cmdCreateIndex = new NpgsqlCommand(command, connection);
        
        cmdCreateIndex.ExecuteNonQuery();
        connection.Close();
    }

    public Task BatchInsert(List<Asset> items)
    {
        lock (_batchLock)
        {
            if (items.Count == 0) return Task.CompletedTask;

            var connection = GetConnection();
            using var writer = connection.BeginBinaryImport(
                $"COPY {_tableName} (CLIENT_ID, ID, MESSAGE_ID, VERSION, CODE, DESCRIPTION, IS_MSI) FROM STDIN (FORMAT BINARY)");

            foreach (var item in items)
            {
                writer.StartRow();
                writer.Write(item.ClientId);
                writer.Write(item.Id ?? 0);
                writer.Write(item.MessageId);
                writer.Write(item.Version);
                writer.Write(item.Code);
                writer.Write(item.Description);
                writer.Write(item.IsMsi);
            }

            writer.Complete();
            connection.Close();
        }

        return Task.CompletedTask;
    }

    public async Task BatchUpdate(List<Asset> assets, int batchSize)
    {
        if (assets.Count == 0) return;

        var batches = CreateBatches(assets, batchSize);

        foreach (var batch in batches)
            await PersistBatch(batch);
    }

    public async Task<int> GetAssetCountForClient(string clientId)
    {
        var connection = GetConnection();
        var result = 0;

        try
        {
            await using var cmd = new NpgsqlCommand(
                $"SELECT COUNT(CLIENT_ID) FROM {_tableName} WHERE CLIENT_ID = '{clientId}'", connection);

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
    
    private async Task PersistBatch(List<Asset> assets)
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
                    var idsToUpdate = assets.Select(i => i.Id).Where(id => id!.HasValue).Select(id => id!.Value).ToList();

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
                    var existingAssets = assets.Where(item => item.Id.HasValue && existingIds.Contains(item.Id.Value)).ToList();

                    if (!existingAssets.Any())
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
                                     $"COPY {tempTableName} (CLIENT_ID, ID, MESSAGE_ID, VERSION, CODE, DESCRIPTION, IS_MSI) FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var asset in existingAssets)
                        {
                            await writer.StartRowAsync();
                            await writer.WriteAsync(asset.ClientId);
                            await writer.WriteAsync(asset.Id!.Value);
                            await writer.WriteAsync(asset.MessageId);
                            await writer.WriteAsync(asset.Version);
                            await writer.WriteAsync(asset.Code);
                            await writer.WriteAsync(asset.Description);
                            await writer.WriteAsync(asset.IsMsi);
                        }
                        
                        await writer.CompleteAsync();
                    }

                    // Update with additional existence check for safety
                    int updatedCount;
                    await using (var cmd = new NpgsqlCommand(
                                     $@"UPDATE {_tableName} AS t
                            SET CODE = temp.CODE,
                                DESCRIPTION = temp.DESCRIPTION,
                                IS_MSI = temp.IS_MSI,
                                VERSION = temp.VERSION,
                                MESSAGE_ID = temp.MESSAGE_ID
                            FROM {tempTableName} AS temp
                            WHERE t.ID = temp.ID", 
                                     connection, transaction))
                    {
                        updatedCount = await cmd.ExecuteNonQueryAsync();
                    }

                    if (updatedCount != existingAssets.Count)
                    {
                        Console.WriteLine($"Warning: Expected to update {existingAssets.Count} records but only updated {updatedCount}");
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
                    Console.WriteLine("Asset: " + e.Message);
                    await transaction.RollbackAsync();
                    throw;
                }
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }
    
    private async Task PersistBatchOld(List<Asset> assets)
    {
        await _flushSemaphore.WaitAsync();

        var maxRetries = 3;
        var retryDelayMilliseconds = 100;

        try
        {
            foreach (var asset in assets)
            {
                // Skip items without IDs
                if (!asset.Id.HasValue)
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
                            checkCmd.Parameters.AddWithValue("id", asset.Id.Value);
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
                                   IS_MSI = @isMsi,                                   
                                   VERSION = @version,
                                   MESSAGE_ID = @messageId
                               WHERE ID = @id", 
                            connection, transaction))
                        {
                            updateCmd.Parameters.AddWithValue("id", asset.Id!.Value);
                            updateCmd.Parameters.AddWithValue("code", asset.Code ?? (object)DBNull.Value);
                            updateCmd.Parameters.AddWithValue("description", asset.Description ?? (object)DBNull.Value);
                            updateCmd.Parameters.AddWithValue("isMsi", asset.IsMsi);                            
                            updateCmd.Parameters.AddWithValue("version", asset.Version);
                            updateCmd.Parameters.AddWithValue("messageId", asset.MessageId);

                            var updatedRows = await updateCmd.ExecuteNonQueryAsync();
                            
                            if (updatedRows != 1)
                            {
                                Console.WriteLine($"Warning: Expected to update 1 record with ID {asset.Id.Value} but updated {updatedRows}");
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
                                $"Transaction failed after multiple retries for item ID {asset.Id.Value} due to deadlock or serialization error.", ex);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"ToDoItem ID {asset.Id.Value}: {e.Message}");
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