using System.Text;
using Newtonsoft.Json;

namespace MqttTestClient.Services.Web;

public class AssetTaskClient
{
    private const string _resourcePath = "/api/assettasks/";
    private readonly HttpClient _httpClient;
    private readonly IWebClientUrlPool _urlPool;
    private string _clientUrl = null!;

    public AssetTaskClient(HttpClient httpClient, IWebClientUrlPool urlPool)
    {
        _urlPool = urlPool;
        _httpClient = httpClient;
    }

    public void Initialise(int? index = 1)
    {
        _clientUrl = _urlPool.GetClientUrl(index);
    }

    public string BuildPostData(string messageId, string clientId, long assetId, string code, string description,
        bool isRfs)
    {
        var asset = new
        {
            messageId,
            clientId,
            assetId,
            code,
            description,
            isRfs
        };
        return JsonConvert.SerializeObject(asset);
    }

    public async Task<string> PostAsync(Guid mqttSessionId, string assetTaskData)
    {
        var url = $"{_clientUrl}{_resourcePath}";
        string jsonPayload = JsonConvert.SerializeObject(new { assetTaskData, mqttSessionId });        
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync(url, payload);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> PutAsync(Guid mqttSessionId, long id, string assetTaskData)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        string jsonPayload = JsonConvert.SerializeObject(new { assetTaskData, mqttSessionId });        
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync(url, payload);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<HttpResponseMessage> GetAllAssetTasksAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}";
        return await _httpClient.GetAsync(url);
    }

    public async Task<string> GetRemoteAssetTaskAsync(long id)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteAssetTaskCountForClientAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}/count/";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteAssetTaskCountAsync()
    {
        var url = $"{_clientUrl}{_resourcePath}count/";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task DeleteAsync(long id, string messageId, Guid mqttSessionId)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        string jsonPayload = JsonConvert.SerializeObject(new { messageId, mqttSessionId });
        var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        
        HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Delete, url)
        {
            Content = content
        };

        await _httpClient.SendAsync(request); 
    }

    public async Task<HttpResponseMessage> DeleteAllAsync()
    {
        var url = $"{_clientUrl}{_resourcePath}";
        return await _httpClient.DeleteAsync(url);
    }

    public async Task<HttpResponseMessage> DeleteForClientAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}";
        return await _httpClient.DeleteAsync(url);
    }
}