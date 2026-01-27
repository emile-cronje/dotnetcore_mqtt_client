using System.Text;
using Newtonsoft.Json;

namespace MqttTestClient.Services.Web;

public class AssetClient
{
    private const string _resourcePath = "/api/assets/";
    private readonly HttpClient _httpClient;
    private readonly IWebClientUrlPool _urlPool;
    private string _clientUrl = string.Empty;
    private EntityContainer? _entityContainer;

    public AssetClient(HttpClient httpClient, IWebClientUrlPool urlPool)
    {
        _urlPool = urlPool;
        _httpClient = httpClient;
    }

    public void SetEntityContainer(EntityContainer? entityContainer)
    {
        _entityContainer = entityContainer;
    }

    public void Initialise(int? index = 1)
    {
        _clientUrl = _urlPool.GetClientUrl(index);
    }

    public string BuildPostData(string messageId, string clientId, string? guid, string code, string description,
        bool isMsi)
    {
        var asset = new
        {
            messageId,
            clientId,
            guid,
            code,
            description,
            isMsi
        };
        return JsonConvert.SerializeObject(asset);
    }

    public async Task<string> PostAsync(Guid mqttSessionId, string assetData)
    {
        var url = $"{_clientUrl}{_resourcePath}";
        string jsonPayload = JsonConvert.SerializeObject(new { assetData, mqttSessionId });        
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync(url, payload);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> PutAsync(Guid mqttSessionId, long id, string assetData, string messageId = "")
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        string jsonPayload = JsonConvert.SerializeObject(new { assetData, mqttSessionId });        
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync(url, payload);
        
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound && !string.IsNullOrEmpty(messageId))
        {
            _entityContainer?.RemoveUpdateMessageIdOn404(messageId);
        }
        
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<HttpResponseMessage> GetAllAssetsAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}";
        return await _httpClient.GetAsync(url);
    }

    public async Task<string> GetRemoteAssetAsync(long id)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteAssetCountForClientAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}/count/";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteAssetCountAsync()
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

        var response = await _httpClient.SendAsync(request);
        
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            _entityContainer?.RemoveDeleteMessageIdOn404(messageId);
        }
    }
    
    public async Task<HttpResponseMessage> DeleteAllAsync()
    {
        var url = $"{_clientUrl}{_resourcePath}";
        var response = await _httpClient.DeleteAsync(url); 
        return response;
    }

    public async Task<HttpResponseMessage> DeleteForClientAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}";
        return await _httpClient.DeleteAsync(url);
    }
}