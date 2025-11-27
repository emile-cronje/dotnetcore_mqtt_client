using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class MeterReadingSqliteDao : SqliteDao, IMeterReadingDao
{
    private readonly string _tableName;

    public MeterReadingSqliteDao(IDbConnectionPool connectionPool) : base(connectionPool)
    {
        _tableName = "meter_reading";
    }

    public async Task<int> AddMeterReading(string clientId, MeterReading meterReading)
    {
        var connection = GetConnection();

        var sql = $"INSERT INTO {_tableName} (CLIENT_ID, REMOTE_ID, VERSION, METER_ID, READING, READING_ON)" +
                  $"VALUES (@CLIENT_ID, @REMOTE_ID, @VERSION, @METER_ID, @READING, @READING_ON);";

        int rowsAffected;

        await using (var cmd = new SqliteCommand(sql, connection))
        {
            cmd.Parameters.AddWithValue("@CLIENT_ID", clientId);
            cmd.Parameters.AddWithValue("@REMOTE_ID", meterReading.Id);
            cmd.Parameters.AddWithValue("@VERSION", 0);
            cmd.Parameters.AddWithValue("@METER_ID", meterReading.MeterId);
            cmd.Parameters.AddWithValue("@READING_ON", meterReading.ReadingOn);

            var decimalParam = new SqliteParameter("@READING", SqliteType.Real)
            {
                Value = meterReading.Reading
            };

            cmd.Parameters.Add(decimalParam);

            rowsAffected = await cmd.ExecuteNonQueryAsync();
        }

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<int> UpdateMeterReading(string clientId, MeterReading meterReading)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = VERSION + 1, READING = @READING, READING_ON = @READING_ON WHERE REMOTE_ID = {meterReading.Id}",
            connection);
        cmd.Parameters.AddWithValue("@READING", meterReading.Reading);
        cmd.Parameters.AddWithValue("@READING_ON", meterReading.ReadingOn);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<MeterReading?> GetMeterReading(long meterReadingId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT remote_id, version, meter_id, reading, reading_on, client_id FROM {_tableName} WHERE remote_id = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", meterReadingId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localMeterReading = new MeterReading(reader.GetInt32(5))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            MeterId = reader.GetInt64(2),
            Reading = reader.GetDecimal(3),
            ReadingOn = reader.GetDateTime(4)
        };

        return localMeterReading;
    }

    public async Task<List<MeterReading>> FetchMeterReadingsForMeter(long meterId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT REMOTE_ID, VERSION, METER_ID, READING, READING_ON, CLIENT_ID FROM {_tableName} WHERE METER_ID = {meterId} ORDER BY READING_ON ASC", connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        ReturnConnection(connection);

        if (!reader.Read()) return new List<MeterReading>();

        var result = new List<MeterReading>();

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

        return result;
    }
    public async Task<int> DeleteMeterReadingById(string clientId, long meterReadingId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE REMOTE_ID = '{meterReadingId}' AND CLIENT_ID = '{clientId}'",
            connection);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        await connection.CloseAsync();

        return rowsAffected;
    }

    public async Task<int> GetMeterReadingVersion(string clientId, long meterReadingId)
    {
        var dbVersion = -1;
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT version from {_tableName} where remote_id = '{meterReadingId}' and client_id = '{clientId}'",
            connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        if (reader.Read()) dbVersion = reader.GetInt32(0);

        ReturnConnection(connection);
        return dbVersion;
    }

    public async Task<int> GetMeterReadingCountForClient(string clientId)
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

    public async Task<int> GetMeterReadingCount()
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
            $"CREATE TABLE {_tableName} (CLIENT_ID TEXT, REMOTE_ID TEXT, VERSION INTEGER, METER_ID INTEGER NOT NULL, READING NUMERIC, READING_ON REAL)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();

        ReturnConnection(connection);
    }

    public Task BatchInsert(List<MeterReading> meterReadings)
    {
        throw new NotImplementedException();
    }

    public Task BatchUpdate(List<MeterReading> meterReadings, int batchSize)
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