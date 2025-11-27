public interface IPgDao
{
    Task<int> GetEntityVersion(long entityId);
    Task<int> GetEntityCount();    
}