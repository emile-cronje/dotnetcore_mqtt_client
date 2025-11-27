namespace MqttTestClient.Services.Web;

public class WebClientUrlPool : IWebClientUrlPool
{
    private readonly List<string> _clientUrlMap;

    public WebClientUrlPool(List<string> clientUrlMap)
    {
        _clientUrlMap = clientUrlMap;
    }

    public void AddClientUrl(string url)
    {
        _clientUrlMap.Add(url);
    }

    public string GetClientUrl(int? index = 1)
    {
        string result;

        if (index.HasValue)
        {
            result = _clientUrlMap[index.Value];
        }
        else
        {
            var rnd = new Random();
            var num = rnd.Next(0, _clientUrlMap.Count);
            result = _clientUrlMap[num];
        }

        return result;
    }
}