using SenseNet.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Diagnostics;
using NLBTester.Executors;

namespace NLBTester;

internal class TestOrchestrator
{
    private readonly IRepositoryCollection _repositoryCollection;
    private readonly WriterExecutor _writerExecutor;
    private readonly ReaderExecutor _readerExecutor;
    private readonly BackupExecutor _backupExecutor;
    private readonly NlbTestOptions _options;
    private readonly ILogger<TestOrchestrator> _logger;

    public TestOrchestrator(IRepositoryCollection repositoryCollection, 
        WriterExecutor writerExecutor,
        ReaderExecutor readerExecutor,
        BackupExecutor backupExecutor,
        IOptions<NlbTestOptions> options,
        ILogger<TestOrchestrator> logger)
    {
        _repositoryCollection = repositoryCollection;
        _writerExecutor = writerExecutor;
        _readerExecutor = readerExecutor;
        _backupExecutor = backupExecutor;
        _options = options.Value;
        _logger = logger;
    }

    public async Task InitializeAsync()
    {
        var repository1 = await _repositoryCollection.GetRepositoryAsync("repo1", CancellationToken.None);
        var repository2 = await _repositoryCollection.GetRepositoryAsync("repo2", CancellationToken.None);

        if (string.IsNullOrEmpty(repository1.Server.Url))
            throw new InvalidOperationException("Repository1 is not configured properly.");

        _logger.LogInformation("Creating test containers if they do not exist...");

        _logger.LogTrace("Ensuring test workspace...");
        await Tools.EnsurePathAsync(TestContents.WorkspacePath, "Workspace", repository1.Server);

        _logger.LogTrace("Ensuring document library...");
        await Tools.EnsurePathAsync(TestContents.DocumentLibraryPath,"DocumentLibrary", repository1.Server);

        _logger.LogTrace("Ensuring repo1 subfolder...");
        await Tools.EnsurePathAsync(RepositoryPath.Combine(TestContents.DocumentLibraryPath, "repo1"), 
            "Folder", repository1.Server);

        if (!string.IsNullOrEmpty(repository1.Server.Url))
        {
            _logger.LogTrace("Ensuring repo2 subfolder...");
            await Tools.EnsurePathAsync(RepositoryPath.Combine(TestContents.DocumentLibraryPath, "repo2"),
                "Folder", repository2.Server);
        }

        _logger.LogTrace("Ensuring task list...");
        await Tools.EnsurePathAsync(TestContents.TaskListPath, "TaskList", repository1.Server);
    }

    public async Task<OperationResult> ExecuteAsync(CancellationToken cancel)
    {
        var totalTime = Stopwatch.StartNew();
        var writerTasks = new List<Task<ExecutionContext>>();

        _logger.LogTrace("Starting WRITE operations...");

        for (var i = 0; i < _options.WriteOperations.ThreadCount; i++)
        {
            writerTasks.Add(RepeatWriterOperationsAsync(cancel));
        }

        try
        {
            // wait a bit before starting the reader operations
            await Task.Delay(5000, cancel);
        }
        catch (OperationCanceledException)
        {
            //TODO: log and calculate values
        }
            
        _logger.LogTrace("Starting READ operations...");

        var readerTasks = new List<Task<ExecutionContext>>();

        for (var i = 0; i < _options.ReadOperations.ThreadCount; i++)
        {
            readerTasks.Add(RepeatReaderOperationsAsync(cancel));
        }

        _logger.LogTrace("Starting BACKUP operation...");

        var backupTask = _backupExecutor.ExecuteAsync(new ExecutionContext
        {
            Repositories = new[] { "repo1" }
        }, _options.Backup, cancel);

        await Task.WhenAll(writerTasks
            .Union(readerTasks)
            .Union(new []{ backupTask }));

        totalTime.Stop();

        var writerContexts = writerTasks.Select(t => t.Result).ToList();
        var readerContexts = readerTasks.Select(t => t.Result).ToList();

        return new OperationResult
        {
            Elapsed = totalTime.Elapsed,
            WriteIterationCount = writerContexts.Sum(c => c.Iteration),
            ReadIterationCount = readerContexts.Sum(c => c.Iteration)
        };
    }

    private async Task<ExecutionContext> RepeatWriterOperationsAsync(CancellationToken cancel)
    {
        //TODO: provide repo names dynamically
        var executionContext = new ExecutionContext
        {
            Repositories = new[] { "repo1", "repo2" }
        };

        // repeat the operation until the test is stopped
        while (!cancel.IsCancellationRequested)
        {
            try
            {
                await _writerExecutor.ExecuteAsync(executionContext, _options.ReadOperations, cancel);
            }
            catch (OperationCanceledException)
            {
                // return gracefully, cancel was requested
            }
        }

        return executionContext;
    }

    private async Task<ExecutionContext> RepeatReaderOperationsAsync(CancellationToken cancel)
    {
        //TODO: provide repo names dynamically
        var executionContext = new ExecutionContext
        {
            Repositories = new[] { "repo1", "repo2" }
        };

        while (!cancel.IsCancellationRequested)
        {
            try
            {
                await _readerExecutor.ExecuteAsync(executionContext, _options.WriteOperations, cancel);
            }
            catch (OperationCanceledException)
            {
                // return gracefully, cancel was requested
            }
        }

        return executionContext;
    }
}