using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqttTestClient.Controllers;
using MqttTestClient.DataAccess;
using MqttTestClient.DataAccess.Pg;
using MqttTestClient.DataAccess.Sqlite;
using MqttTestClient.Services;
using MqttTestClient.Services.Compare;
using MqttTestClient.Services.MessageBroker;
using MqttTestClient.Services.Web;

namespace MqttTestClient;

public abstract class Program
{
    enum WebUrl
    {
        DotNet = 0,                
        NodeTs = 1,
        Python = 2        
    }
    
    public static async Task Main(string[] args)
    {
        var clientIds = new List<string> { "1", "2", "3", "4" };
        //clientIds = new List<string> { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" };        
        clientIds = ["1", "2"];
        //clientIds = new List<string> { "1" };        
        var dotNetHost = "192.168.10.120"; //stm32
        var pythonHost = "192.168.10.120"; //stm32        
        var nodeHost = "192.168.10.120"; //stm32        
        dotNetHost = "192.168.10.116"; //esp32-s3
        dotNetHost = "192.168.10.199"; //picow

        //host = "192.168.10.119"; //stm32        
        dotNetHost = "192.168.10.120"; //stm32 new                
        //host = "192.168.10.121"; //stm32 old                
        //host = "192.168.10.103"; //bbb
        dotNetHost = "192.168.10.184"; //bbb new        
        //host = "192.168.10.135"; //pib plus
        dotNetHost = "localhost";
        dotNetHost = "192.168.10.183"; //pi5

        var pythonPort = 3001;
        var dotNetPort = 8001; // c#        
        var nodePort = 3002; // nodejs - ts        
        var credentials = "foo:bar";
        var entityInsertCount = 2;
        var entityUpdateCount = 1;
        var deletePerEntityTypeCount = 1;        
        var meterReadingsPerMeterCount = 5;
        var tasksPerAssetCount = 1;
        var useSqlite = true;
        var usePg = !useSqlite;
        clientIds = ["1", "2", "3", "4"];        
        clientIds = ["1", "2"];                                                                    
        clientIds = ["1"];        
        //clientIds = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];        

        // entities
        var doItems = true;
        var doAssets = true;
        var doMeters = true;

        // details
        var doAssetTasks = doAssets;        
        var doMeterReadings = doMeters;

        // actions
        var doMeterAdr = true;        
        var doUpdate = true;
        var doDelete = true;
        
        // platform
        bool usePython = false;
        bool useNode = !usePython;        
        bool useDotNet = false;        
        bool usePythonAndNode = usePython && useNode;        
        bool useDotNetAndNode = useDotNet && useNode;                
        bool useDotNetAndPython = useDotNet && usePython;        
        bool useDotNetAndPythonAndNode = useDotNet && usePython && useNode;        
        
        int toDoUrlIndex = (int)WebUrl.NodeTs;
        int assetUrlIndex = (int)WebUrl.NodeTs;
        int assetTaskUrlIndex = (int)WebUrl.NodeTs;       
        int meterUrlIndex = (int)WebUrl.NodeTs;        
        int meterReadingUrlIndex = (int)WebUrl.NodeTs;

        if (usePython)
        {
            toDoUrlIndex = (int)WebUrl.Python;
            assetUrlIndex = (int)WebUrl.Python;
            assetTaskUrlIndex = (int)WebUrl.Python;       
            meterUrlIndex = (int)WebUrl.Python;        
            meterReadingUrlIndex = (int)WebUrl.Python;
        }
        
        if (useNode)
        {
            toDoUrlIndex = (int)WebUrl.NodeTs;
            assetUrlIndex = (int)WebUrl.NodeTs;
            assetTaskUrlIndex = (int)WebUrl.NodeTs;       
            meterUrlIndex = (int)WebUrl.NodeTs;        
            meterReadingUrlIndex = (int)WebUrl.NodeTs;
        }
        
        if (usePythonAndNode)
        {
            toDoUrlIndex = (int)WebUrl.Python;
            assetUrlIndex = (int)WebUrl.NodeTs;
            assetTaskUrlIndex = (int)WebUrl.NodeTs;       
            meterUrlIndex = (int)WebUrl.NodeTs;        
            meterReadingUrlIndex = (int)WebUrl.NodeTs;
        }

        if (useDotNetAndNode)
        {
            toDoUrlIndex = (int)WebUrl.DotNet;
            assetUrlIndex = (int)WebUrl.NodeTs;
            assetTaskUrlIndex = (int)WebUrl.NodeTs;       
            meterUrlIndex = (int)WebUrl.DotNet;        
            meterReadingUrlIndex = (int)WebUrl.DotNet;
        }

        if (useDotNetAndPython)
        {
            toDoUrlIndex = (int)WebUrl.DotNet;
            assetUrlIndex = (int)WebUrl.Python;
            assetTaskUrlIndex = (int)WebUrl.Python;       
            meterUrlIndex = (int)WebUrl.DotNet;        
            meterReadingUrlIndex = (int)WebUrl.DotNet;
        }

        if (useDotNetAndPythonAndNode)
        {
            toDoUrlIndex = (int)WebUrl.NodeTs;
            assetUrlIndex = (int)WebUrl.Python;
            assetTaskUrlIndex = (int)WebUrl.Python;       
            meterUrlIndex = (int)WebUrl.DotNet;        
            meterReadingUrlIndex = (int)WebUrl.DotNet;
        }
        
        var webClientUrls = new List<string>
                    {
                        ($"http://{dotNetHost}:{dotNetPort}"),
                        ($"http://{nodeHost}:{nodePort}"),
                        ($"http://{pythonHost}:{pythonPort}")                        
                    };

        if (usePython)
        {
            pythonHost = "192.168.10.120"; //stm32
            //pythonHost = "192.168.10.115"; //pico-w            
            //pythonHost = "192.168.10.174"; //bbb
            //pythonHost = "192.168.10.198"; //emile-dev            
            //pythonHost = "192.168.10.183"; //pi5 
            pythonPort = 8001;            

            webClientUrls =
            [
                ($"http://{pythonHost}:{pythonPort}"),
                ($"http://{pythonHost}:{pythonPort}"),
                ($"http://{pythonHost}:{pythonPort}")
            ];
        }
        
        if (useNode)
        {
            nodeHost = "localhost";
            //host = "192.168.10.183"; //pi5      
            nodeHost = "192.168.10.174"; //bbb new                          
            //nodeHost = "192.168.10.183"; //pi5
            //nodeHost = "192.168.10.198"; //emile-dev            
            nodePort = 3002;

            webClientUrls =
            [
                ($"http://{nodeHost}:{nodePort}"),
                ($"http://{nodeHost}:{nodePort}"),
                ($"http://{nodeHost}:{nodePort}")
            ];
        }
        
        if (useDotNet)
        {
            dotNetHost = "localhost";
            
            webClientUrls =
            [
                ($"http://{dotNetHost}:{dotNetPort}"),
                ($"http://{dotNetHost}:{dotNetPort}"),
                ($"http://{dotNetHost}:{dotNetPort}")
            ];
        }

        if (useDotNetAndNode)
        {
            dotNetHost = "localhost";
            nodeHost = "192.168.10.174";            
            
            webClientUrls =
            [
                ($"http://{dotNetHost}:{dotNetPort}"),
                ($"http://{dotNetHost}:{dotNetPort}"),
                ($"http://{nodeHost}:{nodePort}")
            ];
        }

        if (useDotNetAndPython)
        {
            dotNetHost = "localhost";
            pythonHost = "192.168.10.120";            
            
            webClientUrls =
            [
                ($"http://{dotNetHost}:{dotNetPort}"),
                ($"http://{dotNetHost}:{dotNetPort}"),
                ($"http://{pythonHost}:{pythonPort}")
            ];
        }
        
        if (usePythonAndNode)
        {
            nodeHost = "192.168.10.174";
            //pythonHost = "192.168.10.115"; //esp32            
            pythonHost = "192.168.10.174"; //stm32
            pythonPort = 3002;            
            pythonPort = 8001;
            
            webClientUrls =
            [
                ($"http://{pythonHost}:{pythonPort}"),
                ($"http://{nodeHost}:{nodePort}"),
                ($"http://{nodeHost}:{nodePort}")
            ];
        }

        if (useDotNetAndPythonAndNode)
        {
            nodeHost = "192.168.10.174";
//            pythonHost = "192.168.10.115"; //esp32            
            pythonHost = "192.168.10.120"; //stm32            
            dotNetHost = "localhost";            
            
            webClientUrls =
            [
                ($"http://{nodeHost}:{nodePort}"),
                ($"http://{pythonHost}:{pythonPort}"),
                ($"http://{dotNetHost}:{dotNetPort}")
            ];
        }
        
        var testEventContainer = new TestEventContainer();
        EntityContainer? toDoContainer = null;        
        EntityContainer? assetContainer = null;                
        EntityContainer? meterContainer = null;        
        EntityContainer? assetTaskContainer = null;                
        EntityContainer? meterReadingContainer = null;                        
        
        if (doItems)
        {
            testEventContainer.EntityContainers.Add(EntityType.ToDoItem, new EntityContainer());
            testEventContainer.EntityContainers.TryGetValue(EntityType.ToDoItem, out toDoContainer!);
        }

        if (doAssets)
        {
            testEventContainer.EntityContainers.Add(EntityType.Asset, new EntityContainer());
            testEventContainer.EntityContainers.TryGetValue(EntityType.Asset, out assetContainer!);            
        }

        if (doAssetTasks)
        {
            testEventContainer.EntityContainers.Add(EntityType.AssetTask, new EntityContainer());            
            testEventContainer.EntityContainers.TryGetValue(EntityType.AssetTask, out assetTaskContainer!);                                    
        }
        
        if (doMeters)
        {
            testEventContainer.EntityContainers.Add(EntityType.Meter, new EntityContainer());            
            testEventContainer.EntityContainers.TryGetValue(EntityType.Meter, out meterContainer!);                        
        }
        
        if (doMeterReadings)
        {
            testEventContainer.EntityContainers.Add(EntityType.MeterReading, new EntityContainer());
            testEventContainer.EntityContainers.TryGetValue(EntityType.MeterReading, out meterReadingContainer!);                                    
        }

        var crudEventContainer = new EntityCrudContainer
        {
            DoUpdate = doUpdate,
            DoDelete = doDelete
        };

        var mqttBrokers = new List<(string, string)>
        {
            ("192.168.10.124", "pib plus"),
            ("192.168.10.135", "pib"),
            ("192.168.10.174", "bbb")            
        };

        var mqttSessionId = Guid.NewGuid();

        // Create a single HttpClientFactory and clients to be shared across services
        var sharedServicesCollection = new ServiceCollection();
        sharedServicesCollection.AddSingleton<IWebClientUrlPool>(new WebClientUrlPool(webClientUrls));
        
        var assetClientBuilder = sharedServicesCollection.AddHttpClient("AssetClient");
        assetClientBuilder.AddStandardResilienceHandler();

        var assetTaskClientBuilder = sharedServicesCollection.AddHttpClient("AssetTaskClient");
        assetTaskClientBuilder.AddStandardResilienceHandler();

        var toDoItemClientBuilder = sharedServicesCollection.AddHttpClient("ToDoItemClient");
        toDoItemClientBuilder.AddStandardResilienceHandler();

        var meterClientBuilder = sharedServicesCollection.AddHttpClient("MeterClient");
        meterClientBuilder.AddStandardResilienceHandler();

        var meterReadingClientBuilder = sharedServicesCollection.AddHttpClient("MeterReadingClient");
        meterReadingClientBuilder.AddStandardResilienceHandler();

        var sharedServiceProvider = sharedServicesCollection.BuildServiceProvider();
        var httpClientFactory = sharedServiceProvider.GetRequiredService<IHttpClientFactory>();
        var webClientUrlPool = sharedServiceProvider.GetRequiredService<IWebClientUrlPool>();

        // Create single instances of each HTTP client
        var assetHttpClient = httpClientFactory.CreateClient("AssetClient");
        assetHttpClient.Timeout = TimeSpan.FromMinutes(2);
        assetHttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes(credentials)));

        var assetTaskHttpClient = httpClientFactory.CreateClient("AssetTaskClient");
        assetTaskHttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes(credentials)));

        var toDoItemHttpClient = httpClientFactory.CreateClient("ToDoItemClient");
        toDoItemHttpClient.Timeout = TimeSpan.FromMinutes(5);
        toDoItemHttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes(credentials)));

        var meterHttpClient = httpClientFactory.CreateClient("MeterClient");
        meterHttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes(credentials)));

        var meterReadingHttpClient = httpClientFactory.CreateClient("MeterReadingClient");
        meterReadingHttpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes(credentials)));

        // Create single instances of each client to be shared
        var toDoItemClient = new ToDoItemClient(toDoItemHttpClient, webClientUrlPool);
        toDoItemClient.Initialise(toDoUrlIndex);
        if (doItems)
            toDoItemClient.SetEntityContainer(toDoContainer);

        var assetClient = new AssetClient(assetHttpClient, webClientUrlPool);
        assetClient.Initialise(assetUrlIndex);
        if (doAssets)
            assetClient.SetEntityContainer(assetContainer);

        var assetTaskClient = new AssetTaskClient(assetTaskHttpClient, webClientUrlPool);
        assetTaskClient.Initialise(assetTaskUrlIndex);
        if (doAssetTasks)
            assetTaskClient.SetEntityContainer(assetTaskContainer);

        var meterClient = new MeterClient(meterHttpClient, webClientUrlPool);
        meterClient.Initialise(meterUrlIndex);
        if (doMeters)
            meterClient.SetEntityContainer(meterContainer);

        var meterReadingClient = new MeterReadingClient(meterReadingHttpClient, webClientUrlPool);
        meterReadingClient.Initialise(meterReadingUrlIndex);
        if (doMeterReadings)
            meterReadingClient.SetEntityContainer(meterReadingContainer);

        var webClientServiceHost = Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                AddDaoService(services, usePg);

                services.AddLogging(builder =>
                {
                    builder.SetMinimumLevel(LogLevel.Error);
                });

                services.AddHostedService(provider =>
                {
                    var toDoDao = provider.GetRequiredService<IToDoItemDao>();
                    var assetDao = provider.GetRequiredService<IAssetDao>();
                    var assetTaskDao = provider.GetRequiredService<IAssetTaskDao>();
                    var meterDao = provider.GetRequiredService<IMeterDao>();
                    var meterReadingDao = provider.GetRequiredService<IMeterReadingDao>();
                    toDoDao.Initialise();
                    assetDao.Initialise();
                    assetTaskDao.Initialise();
                    meterDao.Initialise();
                    meterReadingDao.Initialise();
                    var toDoController = new ToDoItemController(toDoDao, toDoContainer!, testEventContainer);                    
                    var assetController = new AssetController(assetDao, assetContainer!, testEventContainer);                    
                    var assetTaskController = new AssetTaskController(assetTaskDao, assetTaskContainer!, testEventContainer);                    
                    var meterController = new MeterController(meterDao, meterReadingDao, meterContainer!, testEventContainer);                    
                    var meterReadingController = new MeterReadingController(meterReadingDao, meterReadingContainer!, testEventContainer);                    

                    return new WebClientService(clientIds, entityInsertCount, entityUpdateCount,
                        deletePerEntityTypeCount,
                        meterReadingsPerMeterCount, tasksPerAssetCount,
                        testEventContainer, doItems, doAssets, doAssetTasks, doMeters, doMeterReadings,
                        toDoItemClient,
                        assetClient, assetTaskClient, meterClient, meterReadingClient,
                        crudEventContainer, toDoController, assetController, meterController, assetTaskController,
                        meterReadingController, mqttSessionId);
                });
            }).Build();

        var mqttServiceHost = Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                AddDaoService(services, usePg);
                services.AddSingleton<IWebClientUrlPool>(new WebClientUrlPool(webClientUrls));                

                services.AddHostedService(provider =>
                {
                    var toDoDao = provider.GetRequiredService<IToDoItemDao>();
                    var assetDao = provider.GetRequiredService<IAssetDao>();
                    var assetTaskDao = provider.GetRequiredService<IAssetTaskDao>();
                    var meterDao = provider.GetRequiredService<IMeterDao>();
                    var meterReadingDao = provider.GetRequiredService<IMeterReadingDao>();

                    var toDoController = new ToDoItemController(toDoDao, toDoContainer!, testEventContainer);
                    var assetController = new AssetController(assetDao, assetContainer!, testEventContainer);
                    var assetTaskController = new AssetTaskController(assetTaskDao, assetTaskContainer!, testEventContainer);
                    var meterController = new MeterController(meterDao, meterReadingDao, meterContainer!, testEventContainer);
                    var meterReadingController = new MeterReadingController(meterReadingDao, meterReadingContainer!, testEventContainer);

                    return new MessageBrokerService(mqttBrokers, entityUpdateCount, meterReadingsPerMeterCount, testEventContainer,
                        toDoController, assetController, assetTaskController, meterController, meterReadingController,
                        crudEventContainer, tasksPerAssetCount, mqttSessionId);
                });
            }).Build();

        var compareServiceHost = Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                AddDaoService(services, usePg);

                services.AddLogging(builder =>
                {
                    builder.SetMinimumLevel(LogLevel.Error);
                });

                services.AddHostedService(provider =>
                {
                    var toDoDao = provider.GetRequiredService<IToDoItemDao>();
                    var assetDao = provider.GetRequiredService<IAssetDao>();
                    var assetTaskDao = provider.GetRequiredService<IAssetTaskDao>();
                    var meterDao = provider.GetRequiredService<IMeterDao>();
                    var meterReadingDao = provider.GetRequiredService<IMeterReadingDao>();

                    var toDoController = new ToDoItemController(toDoDao, toDoContainer!, testEventContainer);
                    var assetController = new AssetController(assetDao, assetContainer!, testEventContainer);
                    var assetTaskController = new AssetTaskController(assetTaskDao, assetTaskContainer!, testEventContainer);
                    var meterController = new MeterController(meterDao, meterReadingDao, meterContainer!, testEventContainer);
                    var meterReadingController = new MeterReadingController(meterReadingDao, meterReadingContainer!, testEventContainer);

                    return new CompareService(clientIds, testEventContainer, doItems, doAssets,
                        doAssetTasks, toDoController, assetController, assetTaskController, assetClient,
                        assetTaskClient, toDoItemClient, doMeters, doMeterReadings, meterController, meterClient,
                        meterReadingController, meterReadingClient, doDelete, entityInsertCount, doMeterAdr, tasksPerAssetCount,
                        meterReadingsPerMeterCount);
                });
            }).Build();


        var mqttTask = mqttServiceHost.RunAsync();
        var webClientTask = webClientServiceHost.RunAsync();
        var compareTask = compareServiceHost.RunAsync();

        // Calculate total expected operations
        int totalInserts = clientIds.Count * entityInsertCount * (doItems ? 1 : 0) +
                          clientIds.Count * entityInsertCount * (doAssets ? 1 : 0) +
                          clientIds.Count * entityInsertCount * (doMeters ? 1 : 0) +
                          (clientIds.Count * entityInsertCount * (doAssets ? tasksPerAssetCount : 0)) +
                          (clientIds.Count * entityInsertCount * (doMeters ? meterReadingsPerMeterCount : 0));
        
        int totalUpdates = clientIds.Count * entityInsertCount * entityUpdateCount * 
                          ((doItems ? 1 : 0) + (doAssets ? 1 : 0) + (doMeters ? 1 : 0) +
                           (doAssets ? tasksPerAssetCount : 0) + (doMeters ? meterReadingsPerMeterCount : 0));
        
        int totalDeletes = deletePerEntityTypeCount * 
                          ((doItems ? 1 : 0) + (doAssets ? 1 : 0) + (doMeters ? 1 : 0) +
                           (doAssets ? 1 : 0) + (doMeters ? 1 : 0));
        
        int totalOperations = totalInserts + totalUpdates + totalDeletes;

        // // Start progress reporter (fire and forget)
        // _ = Task.Run(async () =>
        // {
        //     Console.WriteLine($"Progress monitoring started. Total operations expected: {totalOperations}");
        //     var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
        //     while (!mqttTask.IsCompleted || !webClientTask.IsCompleted || !compareTask.IsCompleted)
        //     {
        //         await Task.Delay(5000);

        //         int pendingInserts = testEventContainer.EntityContainers
        //             .Sum(x => x.Value?.GetInsertMessageIdsCount() ?? 0);
        //         int pendingUpdates = testEventContainer.EntityContainers
        //             .Sum(x => x.Value?.GetUpdateMessageIdsCount() ?? 0);
        //         int pendingDeletes = testEventContainer.EntityContainers
        //             .Sum(x => x.Value?.GetDeleteMessageIdsCount() ?? 0);
                
        //         int totalPending = pendingInserts + pendingUpdates + pendingDeletes;
        //         int processed = totalOperations - totalPending;
        //         double percentComplete = totalOperations > 0 ? (processed * 100.0) / totalOperations : 0;

        //         var elapsedSeconds = stopwatch.Elapsed.TotalSeconds;
                
        //         Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] Progress: {percentComplete:F1}% ({processed}/{totalOperations}) | " +
        //                         $"Pending - Insert: {pendingInserts}, Update: {pendingUpdates}, Delete: {pendingDeletes} | " +
        //                         $"Elapsed: {elapsedSeconds:F1}s");
        //         Console.Out.Flush();
        //     }
            
        //     stopwatch.Stop();
        //     Console.WriteLine($"Progress monitoring complete. Final elapsed: {stopwatch.Elapsed.TotalSeconds:F1}s");
        // });

        await Task.WhenAll(mqttTask, webClientTask, compareTask);
    }

    private static void AddDaoService(IServiceCollection services, bool usePg = true)
    {
        var dbName = "data.db";
        //dbName = ":memory:";        
        var connectionString = $"Data Source={dbName}";

        services.AddSingleton<IDbConnectionPool>(new DbConnectionPool(connectionString));

        if (usePg)
        {
            services.AddScoped<IToDoItemDao, ToDoItemPgDao>();
            services.AddScoped<IAssetDao, AssetPgDao>();
            services.AddScoped<IAssetTaskDao, AssetTaskPgDao>();
            services.AddScoped<IMeterDao, MeterPgDao>();
            services.AddScoped<IMeterReadingDao, MeterReadingPgDao>();
        }
        else
        {
            services.AddScoped<IToDoItemDao, ToDoItemSqliteDao>();
            services.AddScoped<IAssetDao, AssetSqliteDao>();
            services.AddScoped<IAssetTaskDao, AssetTaskSqliteDao>();
            services.AddScoped<IMeterDao, MeterSqliteDao>();
            services.AddScoped<IMeterReadingDao, MeterReadingSqliteDao>();
        }
    }
}
