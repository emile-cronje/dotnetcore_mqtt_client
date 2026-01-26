using System.Data;
using Npgsql;
using NpgsqlTypes;
using MeterReading = MqttTestClient.Models.MeterReading;

namespace MqttTestClient.DataAccess.Pg;

public class MeterReadingPgDao : PgDao<MeterReading>, IMeterReadingDao
{
    public MeterReadingPgDao(string tableName = "meter_reading") : base(tableName)
    {
    }

    public Task BatchInsert(List<MeterReading> meterReadings)
    {
        lock (_batchLock)
        {
            if (meterReadings.Count == 0) return Task.CompletedTask;

            var connection = GetConnection();
            using var writer = connection.BeginBinaryImport(
                $"COPY {_tableName} (CLIENT_ID, ID, METER_ID, MESSAGE_ID, VERSION, READING, READING_ON) FROM STDIN (FORMAT BINARY)");

            foreach (var reading in meterReadings)
            {
                writer.StartRow();
                writer.Write(reading.ClientId);
                writer.Write(reading.Id ?? 0);
                writer.Write(reading.MeterId);
                writer.Write(reading.MessageId);
                writer.Write(reading.Version);
                writer.Write(reading.Reading);
                writer.Write(reading.ReadingOn);
            }

            writer.Complete();
            connection.Close();
        }

        return Task.CompletedTask;
    }

    public async Task BatchUpdate(List<MeterReading> meterReadings, int batchSize)
    {
        if (meterReadings.Count == 0) return;

        var batches = CreateBatches(meterReadings, batchSize);

        foreach (var batch in batches)
            await PersistBatch(batch);
    }

    public async Task<MeterReading?> GetMeterReading(long meterReadingId)
    {
        var connection = GetConnection();
        MeterReading? result = null;
        await using var cmd = new NpgsqlCommand(
            $"SELECT ID, VERSION, METER_ID, READING, READING_ON, CLIENT_ID, MESSAGE_ID FROM {_tableName} WHERE ID = {meterReadingId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
            result = new MeterReading(reader.GetInt32(5))
            {
                Id = reader.GetInt64(0),
                Version = reader.GetInt32(1),
                MeterId = reader.GetInt64(2),
                Reading = reader.GetDecimal(3),
                ReadingOn = reader.GetDateTime(4),
                MessageId = reader.GetString(6)
            };

        await connection.CloseAsync();
        return result;
    }

    public async Task<List<MeterReading>> FetchMeterReadingsForMeter(long meterId)
    {
        var connection = GetConnection();
        var result = new List<MeterReading>();
        await using var cmd = new NpgsqlCommand(
            $"SELECT ID, VERSION, METER_ID, READING, READING_ON, CLIENT_ID FROM {_tableName} WHERE METER_ID = {meterId} ORDER BY READING_ON ASC",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            var meterReading = new MeterReading(reader.GetInt32(5))
            {
                Id = reader.GetInt64(0),
                Version = reader.GetInt32(1),
                MeterId = reader.GetInt64(2),
                Reading = reader.GetDecimal(3),
                ReadingOn = reader.GetDateTime(4)
            };

            result.Add(meterReading);
        }

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
            $"CREATE TABLE IF NOT EXISTS {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, METER_ID BIGINT NOT NULL, READING NUMERIC(19, 4), READING_ON TIMESTAMP)";
        using var cmdCreate = new NpgsqlCommand(command, connection);

        cmdCreate.ExecuteNonQuery();
        
        command =
            $"CREATE UNIQUE INDEX index_{_tableName}_message_id ON {_tableName}(message_id)";
        using var cmdCreateIndex = new NpgsqlCommand(command, connection);
        
        cmdCreateIndex.ExecuteNonQuery();
        
        connection.Close();
    }

    private async Task PersistBatch(List<MeterReading> meterReadings)
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
                    var idsToUpdate = meterReadings.Select(i => i.Id).Where(id => id.HasValue).Select(id => id!.Value).ToList();

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
                    var existingItems = meterReadings.Where(item => item.Id.HasValue && existingIds.Contains(item.Id.Value)).ToList();

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
                                     $"COPY {tempTableName} (CLIENT_ID, ID, METER_ID, MESSAGE_ID, VERSION, READING, READING_ON) FROM STDIN (FORMAT BINARY)"))
                    {
                        foreach (var assetTask in meterReadings)
                        {
                            await writer.StartRowAsync();
                            await writer.WriteAsync(assetTask.ClientId);
                            await writer.WriteAsync(assetTask.Id ?? 0);
                            await writer.WriteAsync(assetTask.MeterId);
                            await writer.WriteAsync(assetTask.MessageId);
                            await writer.WriteAsync(assetTask.Version);
                            await writer.WriteAsync(assetTask.Reading);
                            await writer.WriteAsync(assetTask.ReadingOn);
                        }

                        await writer.CompleteAsync();
                    }
                    
                    int updatedCount;
                    
                    await using (var cmd = new NpgsqlCommand(
                        $@"UPDATE {_tableName} AS t
                            SET READING = temp.READING,
                                READING_ON = temp.READING_ON,
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
                    Console.WriteLine("Reading: " + e.Message);
                    await transaction.RollbackAsync();
                    throw;
                }
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }
    
    private async Task PersistBatchOld(List<MeterReading> meterReadings)
    {
        await _flushSemaphore.WaitAsync();

        var maxRetries = 3;
        var retryDelayMilliseconds = 100;

        try
        {
            foreach (var reading in meterReadings)
            {
                // Skip items without IDs
                if (!reading.Id.HasValue)
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
                            checkCmd.Parameters.AddWithValue("id", reading.Id.Value);
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
                               SET READING_ON = @readingOn,
                                   READING = @reading,
                                   VERSION = @version,
                                   MESSAGE_ID = @messageId
                               WHERE ID = @id", 
                            connection, transaction))
                        {
                            updateCmd.Parameters.AddWithValue("id", reading.Id.Value);
                            updateCmd.Parameters.AddWithValue("reading", reading.Reading!);
                            updateCmd.Parameters.AddWithValue("readingOn", reading.ReadingOn!);
                            updateCmd.Parameters.AddWithValue("version", reading.Version);
                            updateCmd.Parameters.AddWithValue("messageId", reading.MessageId);

                            var updatedRows = await updateCmd.ExecuteNonQueryAsync();
                            
                            if (updatedRows != 1)
                            {
                                Console.WriteLine($"Warning: Expected to update 1 record with ID {reading.Id.Value} but updated {updatedRows}");
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
                                $"Transaction failed after multiple retries for item ID {reading.Id.Value} due to deadlock or serialization error.", ex);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"ToDoItem ID {reading.Id.Value}: {e.Message}");
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