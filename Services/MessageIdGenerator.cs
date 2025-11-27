namespace MqttTestClient.Services;

public static class MessageIdGenerator
{
    private static long _counter;
    private static long _lastId = 0;
    private static readonly Lock Lock = new Lock();
    private static long _lastTicks = 0;
    private static int _sequence = 0;
    
    public static long GenerateId()
    {
        lock (Lock)
        {
            long ticks = DateTime.UtcNow.Ticks;
        
            if (ticks == _lastTicks)
            {
                _sequence++;
            }
            else
            {
                _sequence = 0;
                _lastTicks = ticks;
            }
        
            return ticks + _sequence;
        }
    }    
    
    public static long GenerateIdOld()
    {
        lock (Lock)
        {
            long id = DateTime.UtcNow.Ticks;
            
            // Ensure uniqueness even if called multiple times in same tick
            if (id <= _lastId)
                id = _lastId + 1;
                
            _lastId = id;
            
            return id;
        }
    }
    
    public static long GenerateIdOld2()
    {
        return Interlocked.Increment(ref _counter);
    }

    // Or with timestamp prefix for sortability
    public static long GenerateTimestampId()
    {
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var counter = Interlocked.Increment(ref _counter);

        return timestamp + counter;
//        return $"{timestamp}-{counter}";
    }
}