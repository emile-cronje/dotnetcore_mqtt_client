using Microsoft.Data.Sqlite;
using MqttTestClient.Models;
using MqttTestClient.Services;

namespace MqttTestClient.DataAccess.Sqlite;

public class MeterReadingSqliteDao : SqliteDao, IMeterReadingDao
{
    private readonly string _tableName;

    public MeterReadingSqliteDao(IDbConnectionPool connectionPool, string tableName = "meter_reading") : base(connectionPool)
    {
        _tableName = tableName;
    }

    public async Task<int> AddMeterReading(string clientId, MeterReading meterReading)
    {
        var connection = GetConnection();

        var sql = $"INSERT INTO {_tableName} (ID, VERSION, CLIENT_ID, MESSAGE_ID, METER_ID, READING, READING_ON)" +
                  $"VALUES (@ID, @VERSION, @CLIENT_ID, @MESSAGE_ID, @METER_ID, @READING, @READING_ON);";

        int rowsAffected;

        await using (var cmd = new SqliteCommand(sql, connection))
        {
            cmd.Parameters.AddWithValue("@ID", meterReading.Id);
            cmd.Parameters.AddWithValue("@VERSION", 0);
            cmd.Parameters.AddWithValue("@CLIENT_ID", clientId);
            cmd.Parameters.AddWithValue("@MESSAGE_ID", meterReading.MessageId);
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
            $"UPDATE {_tableName} SET VERSION = VERSION + 1, MESSAGE_ID = @MESSAGE_ID, READING = @READING, READING_ON = @READING_ON WHERE ID = {meterReading.Id}",
            connection);
        cmd.Parameters.AddWithValue("@MESSAGE_ID", meterReading.MessageId);
        cmd.Parameters.AddWithValue("@READING", meterReading.Reading);
        cmd.Parameters.AddWithValue("@READING_ON", meterReading.ReadingOn);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();

        ReturnConnection(connection);
        return rowsAffected;
    }

    public async Task<int> UpdateMeterReadingOld(string clientId, MeterReading meterReading)
    {
        var connection = GetConnection();

        // Fetch the current version from the database
        await using var versionCmd = new SqliteCommand(
            $"SELECT VERSION FROM {_tableName} WHERE ID = @ID",
            connection);
        versionCmd.Parameters.AddWithValue("@ID", meterReading.Id);
        
        await using var versionReader = await versionCmd.ExecuteReaderAsync();
        if (!versionReader.Read())
        {
            ReturnConnection(connection);
            throw new InvalidOperationException($"MeterReading ID {meterReading.Id} not found");
        }
        
        var currentVersion = versionReader.GetInt32(0);

        // Now update using the current version from the database
        await using var cmd = new SqliteCommand(
            $"UPDATE {_tableName} SET VERSION = VERSION + 1, MESSAGE_ID = @MESSAGE_ID, READING = @READING, READING_ON = @READING_ON WHERE ID = @ID AND VERSION = @VERSION",
            connection);
        cmd.Parameters.AddWithValue("@ID", meterReading.Id);
        cmd.Parameters.AddWithValue("@VERSION", currentVersion);
        cmd.Parameters.AddWithValue("@MESSAGE_ID", meterReading.MessageId);
        cmd.Parameters.AddWithValue("@READING", meterReading.Reading);
        cmd.Parameters.AddWithValue("@READING_ON", meterReading.ReadingOn);

        var rowsAffected = await cmd.ExecuteNonQueryAsync();
        ReturnConnection(connection);
        
        if (rowsAffected == 0)
        {
            throw new InvalidOperationException($"Version mismatch for MeterReading ID {meterReading.Id}. Current DB version {currentVersion}, in-memory version {meterReading.Version}");
        }

        return rowsAffected;
    }

    public async Task<MeterReading?> GetMeterReading(long meterReadingId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT ID, VERSION, MESSAGE_ID, METER_ID, READING, READING_ON, CLIENT_ID FROM {_tableName} WHERE ID = @id",
            connection);
        cmd.Parameters.AddWithValue("@id", meterReadingId);

        await using var reader = await cmd.ExecuteReaderAsync();
        ReturnConnection(connection);

        if (!reader.Read()) return null;

        var localMeterReading = new MeterReading(reader.GetInt32(6))
        {
            Id = reader.GetInt64(0),
            Version = reader.GetInt32(1),
            MessageId = reader.GetString(2),
            MeterId = reader.GetInt64(3),
            Reading = reader.GetDecimal(4),
            ReadingOn = reader.GetDateTime(5)
        };

        return localMeterReading;
    }

    public async Task<List<MeterReading>> FetchMeterReadingsForMeter(long meterId)
    {
        var connection = GetConnection();

        await using var cmd = new SqliteCommand(
            $"SELECT ID, VERSION, MESSAGE_ID, METER_ID, READING, READING_ON, CLIENT_ID FROM {_tableName} WHERE METER_ID = {meterId} ORDER BY READING_ON ASC", connection);

        await using var reader = await cmd.ExecuteReaderAsync();

        ReturnConnection(connection);

        if (!reader.Read()) return new List<MeterReading>();

        var result = new List<MeterReading>();

        while (await reader.ReadAsync())
        {
            var meterReading = new MeterReading(reader.GetInt32(6))
            {
                Id = reader.GetInt64(0),
                Version = reader.GetInt32(1),
                MessageId = reader.GetString(2),
                MeterId = reader.GetInt64(3),
                Reading = reader.GetDecimal(4),
                ReadingOn = reader.GetDateTime(5)
            };

            result.Add(meterReading);
        }

        return result;
    }
    public async Task<int> DeleteMeterReadingById(string clientId, long meterReadingId)
    {
        var connection = GetConnection();
        await using var cmd = new SqliteCommand(
            $"DELETE FROM {_tableName} WHERE ID = {meterReadingId} AND CLIENT_ID = {clientId}",
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
            $"SELECT VERSION FROM {_tableName} WHERE ID = {meterReadingId} AND CLIENT_ID = {clientId}",
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
            new SqliteCommand($"SELECT COUNT(*) FROM {_tableName} WHERE CLIENT_ID = {clientId}", connection);
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
            $"CREATE TABLE {_tableName} (ID BIGINT, VERSION INTEGER, CLIENT_ID INTEGER, MESSAGE_ID TEXT, METER_ID BIGINT NOT NULL, READING NUMERIC, READING_ON REAL)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        command = $"CREATE UNIQUE INDEX idx_{_tableName}_message_id ON {_tableName}(MESSAGE_ID)";
        cmd = new SqliteCommand(command, connection);
        cmd.ExecuteNonQuery();
        
        InitializeWalMode(connection);

        ReturnConnection(connection);
    }

    public async Task BatchInsert(List<MeterReading> meterReadings)
    {
        foreach (var meterReading in meterReadings) await AddMeterReading(meterReading.ClientId.ToString(), meterReading);
    }

    public async Task BatchUpdate(List<MeterReading> meterReadings, int batchSize)
    {
        foreach (var meterReading in meterReadings) await UpdateMeterReading(meterReading.ClientId.ToString(), meterReading);
    }

    public async Task BatchDelete(IEnumerable<long> ids)
    {
        foreach (var id in ids)
        {
            var connection = GetConnection();
            await using var cmd = new SqliteCommand(
                $"DELETE FROM {_tableName} WHERE ID = {id}",
                connection);
            await cmd.ExecuteNonQueryAsync();
            ReturnConnection(connection);
        }
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