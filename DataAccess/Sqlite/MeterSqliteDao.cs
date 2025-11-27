using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class MeterSqliteDao : SqliteDao, IMeterDao
{
    private readonly string _tableName;

    public MeterSqliteDao(IDbConnectionPool connectionPool) : base(connectionPool)
    {
        _tableName = "meter";
    }

    public async Task<int> AddMeter(string clientId, Meter meter)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"INSERT INTO {_tableName} (CLIENT_ID, REMOTE_ID, VERSION, CODE, DESCRIPTION, IS_PAUSED) VALUES ('{clientId}', '{meter.Id}', {meter.Version}, '{meter.Code}', '{meter.Description}', {meter.IsPaused})",
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
            $"UPDATE {_tableName} SET VERSION = {meter.Version}, CODE = '{meter.Code}', DESCRIPTION = '{meter.Description}', IS_PAUSED = {meter.IsPaused} WHERE REMOTE_ID = '{meter.Id}'",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<Meter?> GetMeter(long meterId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT remote_id, version, code, description, is_paused, client_id FROM {_tableName} WHERE remote_id = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", meterId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localMeter = new Meter(reader.GetInt32(5))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            Code = reader.GetString(2),
            Description = reader.GetString(3),
            IsPaused = reader.GetBoolean(4)
        };

        return localMeter;
    }

    public async Task<int> GetMeterVersion(long meterId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT version from {_tableName} where remote_id = '{meterId}'",
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
            $"DELETE FROM {_tableName} WHERE REMOTE_ID = '{meterId}'",
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
            new SqliteCommand($"SELECT count(*) from {_tableName} where client_id = '{clientId}'", connection);
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
            new SqliteCommand($"SELECT count(*) from {_tableName}", connection);
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
            $"CREATE TABLE {_tableName} (CLIENT_ID TEXT, REMOTE_ID TEXT, VERSION INTEGER, CODE TEXT, DESCRIPTION TEXT, IS_PAUSED INTEGER)";
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

    public Task<int> GetEntityVersion(long entityId)
    {
        throw new NotImplementedException();
    }

    public Task<int> GetEntityCount()
    {
        throw new NotImplementedException();
    }
}