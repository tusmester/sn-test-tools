using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NLBTester;
using SenseNet.Client;
using SenseNet.Extensions.DependencyInjection;

var host = Host.CreateDefaultBuilder()
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
            .AddTransient<TestExecutor>()
            .Configure<NlbTestOptions>(nlbTestOptions =>
            {
                context.Configuration.GetSection("NlbTest").Bind(nlbTestOptions);
            });
    }).Build();

var repositoryCollection = host.Services.GetRequiredService<IRepositoryCollection>();
var repository1 = await repositoryCollection.GetRepositoryAsync("repo1", CancellationToken.None);
var repository2 = await repositoryCollection.GetRepositoryAsync("repo2", CancellationToken.None);
var nlbTestOptions = host.Services.GetRequiredService<IOptions<NlbTestOptions>>().Value;

Console.WriteLine("Creating test containers if they do not exist...");

await Tools.EnsurePathAsync(TestExecutor.TestContainerPath, "DocumentLibrary", repository1.Server);
await Tools.EnsurePathAsync(RepositoryPath.Combine(TestExecutor.TestContainerPath, "repo1"), "Folder", repository1.Server);
await Tools.EnsurePathAsync(RepositoryPath.Combine(TestExecutor.TestContainerPath, "repo2"), "Folder", repository2.Server);

Console.WriteLine($"Starting test on {nlbTestOptions.ThreadCount} thread{(nlbTestOptions.ThreadCount == 1 ? string.Empty : "s")}.");
Console.WriteLine("Press any key to stop the test.");
Console.WriteLine();

var cancelTokenSource = new CancellationTokenSource();
var cancelToken = cancelTokenSource.Token;

var totalTime = Stopwatch.StartNew();
var tasks = new List<Task>();

// start the test in one or more threads
for (var i = 0; i < nlbTestOptions.ThreadCount; i++)
{
    tasks.Add(RepeatAsync(cancelToken));
}

Console.ReadKey();

cancelTokenSource.Cancel();

await Task.WhenAll(tasks);

Console.WriteLine($"Total time: {totalTime.Elapsed}");

async Task RepeatAsync(CancellationToken cancel)
{
    var executor = host.Services.GetRequiredService<TestExecutor>();

    while (!cancel.IsCancellationRequested)
    {
        await executor.ExecuteAsync(new[] { "repo1", "repo2" }, cancel);
    }
}