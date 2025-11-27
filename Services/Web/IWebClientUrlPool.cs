namespace MqttTestClient.Services.Web;

public interface IWebClientUrlPool
{
    void AddClientUrl(string url);
    string GetClientUrl(int? index = 1);
}