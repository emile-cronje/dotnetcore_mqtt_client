using System.Text;
using Newtonsoft.Json;

namespace MqttTestClient.Services.Web;

public class MeterReadingClient
{
    private const string _resourcePath = "/api/meterreadings/";
    private readonly HttpClient _httpClient;
    private readonly IWebClientUrlPool _urlPool;
    private string _clientUrl = string.Empty;

    public MeterReadingClient(HttpClient httpClient, IWebClientUrlPool urlPool)
    {
        _urlPool = urlPool;
        _httpClient = httpClient;
    }

    public void Initialise(int? index = 1)
    {
        _clientUrl = _urlPool.GetClientUrl(index);
    }

    public string BuildPostData(string messageId, string clientId, long meterId, decimal reading, DateTime readingOn)
    {
        var meterReading = new
        {
            messageId,            
            clientId,
            meterId,
            reading,
            readingOn = readingOn.ToString("o")
        };
        return JsonConvert.SerializeObject(meterReading);
    }

    public async Task<string> PostAsync(Guid mqttSessionId, string meterReadingData)
    {
        var url = $"{_clientUrl}{_resourcePath}";
        string jsonPayload = JsonConvert.SerializeObject(new { meterReadingData, mqttSessionId });                
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync(url, payload);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> PutAsync(Guid mqttSessionId, long id, string meterReadingData)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        string jsonPayload = JsonConvert.SerializeObject(new { meterReadingData, mqttSessionId });                
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync(url, payload);
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
    
    public async Task<HttpResponseMessage> GetAllMeterReadingsAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}";
        return await _httpClient.GetAsync(url);
    }

    public async Task<string> GetRemoteMeterReadingAsync(long id)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteMeterReadingCountForClientAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}/count/";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteMeterReadingCountAsync()
    {
        var url = $"{_clientUrl}{_resourcePath}count/";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task DeleteReadingAsync(long id)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        await _httpClient.DeleteAsync(url);
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