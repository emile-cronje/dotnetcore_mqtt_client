using System.Data;
using Npgsql;
using NpgsqlTypes;
using Meter = MqttTestClient.Models.Meter;

namespace MqttTestClient.DataAccess.Pg;

public class MeterPgDao : PgDao<Meter>, IMeterDao
{
    public MeterPgDao(string tableName = "meter") : base(tableName)
    {
    }

    public async Task<Meter?> GetMeter(long meterId)
    {
        var connection = GetConnection();
        Meter? result = null;
        await using var cmd = new NpgsqlCommand(
            $"SELECT ID, VERSION, CODE, DESCRIPTION, IS_PAUSED, ADR, CLIENT_ID, MESSAGE_ID FROM {_tableName} WHERE ID = {meterId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
            result = new Meter(reader.GetInt32(6))
            {
                Id = reader.GetInt64(0),
                Version = reader.GetInt32(1),
                Code = reader.GetString(2),
                Description = reader.GetString(3),
                IsPaused = reader.GetBoolean(4),
                Adr = reader.GetDecimal(5),
                MessageId = reader.GetString(7)                
            };

        await connection.CloseAsync();
        return result;
    }

    public async Task<decimal?> GetMeterAdr(long meterId)
    {
        var connection = GetConnection();
        
        decimal? result = 0;
        await using var cmd = new NpgsqlCommand(
            $"SELECT COALESCE(calculate_average_daily_rate({meterId}), 0);", connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
            result = reader.GetDecimal(0);

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
            $"CREATE TABLE IF NOT EXISTS {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, CODE TEXT, DESCRIPTION TEXT, IS_PAUSED BOOLEAN, ADR DECIMAL)";
        using var cmdCreate = new NpgsqlCommand(command, connection);

        cmdCreate.ExecuteNonQuery();
        
        command =
            $"CREATE UNIQUE INDEX index_{_tableName}_message_id ON {_tableName}(message_id)";
        using var cmdCreateIndex = new NpgsqlCommand(command, connection);
        
        cmdCreateIndex.ExecuteNonQuery();
        
        CreateFunctions(connection);
        connection.Close();
    }

    public Task BatchInsert(List<Meter> meters)
    {
        lock (_batchLock)
        {
            if (meters.Count == 0) return Task.CompletedTask;

            var connection = GetConnection();
            using var writer = connection.BeginBinaryImport(
                $"COPY {_tableName} (CLIENT_ID, ID, MESSAGE_ID, VERSION, CODE, DESCRIPTION, IS_PAUSED, ADR) FROM STDIN (FORMAT BINARY)");

            foreach (var meter in meters)
            {
                writer.StartRow();
                writer.Write(meter.ClientId);
                writer.Write(meter.Id ?? 0);
                writer.Write(meter.MessageId);
                writer.Write(meter.Version);
                writer.Write(meter.Code);
                writer.Write(meter.Description);
                writer.Write(meter.IsPaused);
                writer.Write(meter.Adr);                
            }

            writer.Complete();
            connection.Close();
        }

        return Task.CompletedTask;
    }

    public async Task BatchUpdate(List<Meter> meters, int batchSize)
    {
        if (meters.Count == 0) return;

        var batches = CreateBatches(meters, batchSize);

        foreach (var batch in batches)
            await PersistBatch(batch);
    }

    private void CreateFunctions(NpgsqlConnection connection)
    {
        var createFunctionSql = @"
CREATE OR REPLACE FUNCTION calculate_average_daily_rate(p_meter_id bigint)
RETURNS NUMERIC AS $$
DECLARE
avg_daily_rate NUMERIC;
BEGIN
-- Calculate the average daily rate
SELECT AVG(COALESCE(daily_rate, 0)) INTO avg_daily_rate  -- Replace NULL with 0 for averaging
FROM (
SELECT
(reading - LAG(reading) OVER (ORDER BY reading_on))::NUMERIC /
NULLIF((reading_on::DATE - LAG(reading_on) OVER (ORDER BY reading_on)::DATE), 0) AS daily_rate
FROM meter_reading
WHERE meter_id = p_meter_id
ORDER BY id -- consider if you need this ORDER BY, it might not be required and impacts performance
) AS daily_rates;

   -- Return the calculated average daily rate
   RETURN avg_daily_rate;
END;
$$ LANGUAGE plpgsql;
        ";

        try
        {
            using (connection)
            {
                // Create a command to execute the SQL
                using (var command = new NpgsqlCommand(createFunctionSql, connection))
                {
                    // Execute the command
                    command.ExecuteNonQuery();
                    Console.WriteLine("Function created successfully.");
                }
            }
        }
        catch (Exception ex)
        {
            // Handle any errors that occur during the execution
            Console.WriteLine("An error occurred: " + ex.Message);
        }
    }    

    private async Task PersistBatch(List<Meter> meters)
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
                    var idsToUpdate = meters.Select(i => i.Id).Where(id => id.HasValue).Select(id => id.Value).ToList();

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
                    var existingItems = meters.Where(item => item.Id.HasValue && existingIds.Contains(item.Id.Value)).ToList();

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
                                     $"COPY {tempTableName} (CLIENT_ID, ID, MESSAGE_ID, VERSION, CODE, DESCRIPTION, IS_PAUSED) FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var item in meters)
                        {
                            await writer.StartRowAsync();
                            await writer.WriteAsync(item.ClientId);
                            await writer.WriteAsync(item.Id ?? 0);
                            await writer.WriteAsync(item.MessageId);
                            await writer.WriteAsync(item.Version);
                            await writer.WriteAsync(item.Code);
                            await writer.WriteAsync(item.Description);
                            await writer.WriteAsync(item.IsPaused);
                        }

                        await writer.CompleteAsync();
                    }
                    
                    int updatedCount;
 
                    await using (var cmd = new NpgsqlCommand(
                                     $@"UPDATE {_tableName} AS t
                            SET CODE = temp.CODE,
                                DESCRIPTION = temp.DESCRIPTION,
                                IS_PAUSED = temp.IS_PAUSED,
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
                    Console.WriteLine("Meter: " + e.Message);
                    await transaction.RollbackAsync();
                    throw;
                }
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }
    
    private async Task PersistBatchOld(List<Meter> meters)
    {
        await _flushSemaphore.WaitAsync();

        var maxRetries = 3;
        var retryDelayMilliseconds = 100;

        try
        {
            foreach (var meter in meters)
            {
                // Skip items without IDs
                if (!meter.Id.HasValue)
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
                            checkCmd.Parameters.AddWithValue("id", meter.Id.Value);
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
                                   IS_PAUSED = @isPaused,                                   
                                   VERSION = @version,
                                   MESSAGE_ID = @messageId
                               WHERE ID = @id", 
                            connection, transaction))
                        {
                            updateCmd.Parameters.AddWithValue("id", meter.Id.Value);
                            updateCmd.Parameters.AddWithValue("code", meter.Code);
                            updateCmd.Parameters.AddWithValue("description", meter.Description);
                            updateCmd.Parameters.AddWithValue("isPaused", meter.IsPaused);                            
                            updateCmd.Parameters.AddWithValue("version", meter.Version);
                            updateCmd.Parameters.AddWithValue("messageId", meter.MessageId);

                            var updatedRows = await updateCmd.ExecuteNonQueryAsync();
                            
                            if (updatedRows != 1)
                            {
                                Console.WriteLine($"Warning: Expected to update 1 record with ID {meter.Id.Value} but updated {updatedRows}");
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
                                $"Transaction failed after multiple retries for item ID {meter.Id.Value} due to deadlock or serialization error.", ex);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"ToDoItem ID {meter.Id.Value}: {e.Message}");
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