using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLBTester;
using NLBTester.Executors;
using SenseNet.Extensions.DependencyInjection;
using Serilog;

var host = Host.CreateDefaultBuilder()
    .UseSerilog((context, configuration) =>
    {
        configuration.ReadFrom.Configuration(context.Configuration);
    })
    .ConfigureServices((context, services) =>
    {
        // add the sensenet client feature and configure the repository
        services
            .AddLogging(logging => logging.AddConsole())
            .AddSenseNetClient()
            .ConfigureSenseNetRepository("repo1",
                repositoryOptions =>
            {
                context.Configuration.GetSection("sensenet:repository1").Bind(repositoryOptions);
            })
            .ConfigureSenseNetRepository("repo2",
            repositoryOptions =>
            {
                context.Configuration.GetSection("sensenet:repository2").Bind(repositoryOptions);
            })
            .AddSingleton<ReaderExecutor>()
            .AddSingleton<WriterExecutor>()
            .AddSingleton<BackupExecutor>()
            .AddSingleton<TestOrchestrator>()
            .Configure<NlbTestOptions>(nlbTestOptions =>
            {
                context.Configuration.GetSection("NlbTest").Bind(nlbTestOptions);
            });
    }).Build();

Console.WriteLine("===========================================================================");
Console.WriteLine("Starting NLB test");
Console.WriteLine("Press any key to stop the test.");
Console.WriteLine("===========================================================================");
Console.WriteLine();

var orchestrator = host.Services.GetRequiredService<TestOrchestrator>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();

await orchestrator.InitializeAsync();

var cancelTokenSource = new CancellationTokenSource();
var orchestrationTask = orchestrator.ExecuteAsync(cancelTokenSource.Token);

Console.ReadKey();

cancelTokenSource.Cancel();

await orchestrationTask;

var result = orchestrationTask.Result;

logger.LogInformation("TOTAL TIME: {Elapsed}", result.Elapsed);
logger.LogInformation("TOTAL # OF WRITE ITERATIONS: {IterationCount}", result.WriteIterationCount);
logger.LogInformation("TOTAL # OF READ ITERATIONS: {IterationCount}", result.ReadIterationCount);

