using System.Globalization;
using System.Text;
using Newtonsoft.Json;

namespace MqttTestClient.Services.Web;

public class MeterClient
{
    private const string _resourcePath = "/api/meters/";
    private readonly HttpClient _httpClient;
    private readonly IWebClientUrlPool _urlPool;
    private string _clientUrl = string.Empty;

    public MeterClient(HttpClient httpClient, IWebClientUrlPool urlPool)
    {
        _urlPool = urlPool;
        _httpClient = httpClient;
    }

    public void Initialise(int? index = 1)
    {
        _clientUrl = _urlPool.GetClientUrl(index);
    }

    public string BuildPostData(string messageId, string clientId, string guid, string code, string description,
        bool isPaused)
    {
        var meter = new
        {
            messageId,
            clientId,
            guid,
            code,
            description,
            isPaused
        };
        return JsonConvert.SerializeObject(meter);
    }

    public async Task<string> PostAsync(Guid mqttSessionId, string meterData)
    {
        var url = $"{_clientUrl}{_resourcePath}";
        string jsonPayload = JsonConvert.SerializeObject(new { meterData, mqttSessionId });        
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync(url, payload);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> PutAsync(Guid mqttSessionId, long id, string meterData)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        string jsonPayload = JsonConvert.SerializeObject(new { meterData, mqttSessionId });        
        var payload = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync(url, payload);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<HttpResponseMessage> GetAllMetersAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}";
        return await _httpClient.GetAsync(url);
    }

    public async Task<string> GetRemoteMeterAsync(long id)
    {
        var url = $"{_clientUrl}{_resourcePath}{id}";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteMeterCountForClientAsync(string clientId)
    {
        var url = $"{_clientUrl}{_resourcePath}{clientId}/count/";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<string> GetRemoteMeterCountAsync()
    {
        var url = $"{_clientUrl}{_resourcePath}count/";
        var response = await _httpClient.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }

    public async Task<decimal?> GetRemoteMeterAdrAsync(long meterId)
    {
        var url = $"{_clientUrl}{_resourcePath}{meterId}/adr/";
        var response = await _httpClient.GetAsync(url);
        var adrString = await response.Content.ReadAsStringAsync();
        var style = NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign;
        decimal? adr = decimal.Parse(adrString, style, CultureInfo.InvariantCulture);

        return adr;
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