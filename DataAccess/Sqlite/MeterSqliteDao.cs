using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class MeterSqliteDao : SqliteDao, IMeterDao
{
    private readonly string _tableName;

    public MeterSqliteDao(IDbConnectionPool connectionPool, string tableName = "meter") : base(connectionPool)
    {
        _tableName = tableName;
    }

    public async Task<int> AddMeter(string clientId, Meter meter)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (ID, VERSION, CLIENT_ID, MESSAGE_ID, CODE, DESCRIPTION, IS_PAUSED) VALUES ({meter.Id}, {meter.Version}, {clientId}, '{meter.MessageId}', '{meter.Code}', '{meter.Description}', {meter.IsPaused})",
            connection);

        int rowsAffected;

        try
        {
            rowsAffected = await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<int> UpdateMeter(Meter meter)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = {meter.Version}, MESSAGE_ID = '{meter.MessageId}', CODE = '{meter.Code}', DESCRIPTION = '{meter.Description}', IS_PAUSED = {meter.IsPaused} WHERE ID = {meter.Id}",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<Meter?> GetMeter(long meterId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT ID, VERSION, MESSAGE_ID, CODE, DESCRIPTION, IS_PAUSED, CLIENT_ID FROM {_tableName} WHERE ID = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", meterId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localMeter = new Meter(reader.GetInt32(6))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            MessageId = reader.GetString(2),
            Code = reader.GetString(3),
            Description = reader.GetString(4),
            IsPaused = reader.GetBoolean(5)
        };

        return localMeter;
    }

    public async Task<int> GetMeterVersion(long meterId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = {meterId}",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (reader.Read()) dbVersion = reader.GetInt32(0);

        ReturnConnection(connection);
        return dbVersion;
    }

    public async Task<int> DeleteMeter(long meterId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE ID = {meterId}",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();

        return rowsAffected;
    }

    public async Task<int> GetMeterCountForClient(string clientId)
    {
        var itemCount = 0;
        var connection = GetConnection();

        await using var cmdCount =
            new SqliteCommand($"SELECT COUNT(*) FROM {_tableName} WHERE CLIENT_ID = {clientId}", connection);
        await using var readerCount = await cmdCount.ExecuteReaderAsync();

        if (readerCount.Read())
            itemCount = readerCount.GetInt32(0);

        ReturnConnection(connection);
        return itemCount;
    }

    public async Task<int> GetMeterCount()
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

    public void Initialise()
    {
        var connection = GetConnection();

        var command = $"DROP TABLE IF EXISTS {_tableName}";
        var cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        command =
            $"CREATE TABLE {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, CODE TEXT, DESCRIPTION TEXT, IS_PAUSED INTEGER)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        command = $"CREATE UNIQUE INDEX idx_{_tableName}_message_id ON {_tableName}(MESSAGE_ID)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        ReturnConnection(connection);
    }

    public async Task<decimal?> GetMeterAdr(long meterId)
    {
        decimal? adr = 0;
        var connection = GetConnection();

        var sql = @"
                    SELECT COALESCE(AVG(daily_rate), 0) AS average_daily_rate
                    FROM (
                        SELECT
                            (reading - LAG(reading) OVER (ORDER BY reading_on)) /
                            (JULIANDAY(reading_on) - JULIANDAY(LAG(reading_on) OVER (ORDER BY reading_on))) AS daily_rate
                        FROM meter_reading
                        WHERE meter_id = @meterId
                    ) AS daily_rates";

        await using (var cmd = new SqliteCommand(sql, connection))
        {
            cmd.Parameters.AddWithValue("@meterId", meterId);

            var result = await cmd.ExecuteScalarAsync();

            if (result != DBNull.Value) adr = Convert.ToDecimal(result);
        }

        ReturnConnection(connection);
        return adr;
    }

    public Task BatchInsert(List<Meter> meters)
    {
        throw new NotImplementedException();
    }

    public Task BatchUpdate(List<Meter> meters, int batchSize)
    {
        throw new NotImplementedException();
    }

    public Task BatchDelete(IEnumerable<long> ids)
    {
        throw new NotImplementedException();
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
        var connection = GetConnection();

        var cmd = new SqliteCommand($"SELECT COUNT(*) FROM {_tableName}", connection);

        try
        {
            await using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();
            var count = reader.GetInt32(0);
            ReturnConnection(connection);
            return count;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}