using MqttTestClient.Services;

namespace MqttTestClient.Controllers;

public abstract class EntityController
{
    protected const int BatchSize = 100;
    protected TimeSpan FlushTimeOut = TimeSpan.FromSeconds(5);
    public readonly EntityContainer? EntityContainer;
    protected readonly TestEventContainer TestEventContainer;
    protected DateTime DeleteBatchStartTime;
    protected DateTime InsertBatchStartTime;
    protected Timer FlushTimer = null!;
    protected DateTime UpdateBatchStartTime;
    protected readonly SemaphoreSlim InsertBatchLockSem = new(1, 1);    
    protected readonly SemaphoreSlim InsertFlushBatchLockSem = new(1, 1);    
    protected readonly SemaphoreSlim UpdateBatchLockSem = new(1, 1);    
    protected readonly SemaphoreSlim UpdateFlushBatchLockSem = new(1, 1);    
    protected readonly SemaphoreSlim DeleteBatchLockSem = new(1, 1);    
    protected readonly SemaphoreSlim DeleteFlushBatchLockSem = new(1, 1);    

    protected EntityController(EntityContainer entityContainer, TestEventContainer testEventContainer)
    {
        EntityContainer = entityContainer;
        TestEventContainer = testEventContainer;
    }

    public string GetNextEntityMessageId => Ulid.NewUlid().ToString();
}