using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SenseNet.Client;

namespace NLBTester.Executors;

internal abstract class ExecutorBase
{
    public IRepositoryCollection RepositoryCollection { get; }
    public NlbTestOptions Options { get; }
    protected ILogger<ExecutorBase> Logger { get; }

    protected virtual string ExecutorName => GetType().Name;

    protected ExecutorBase(IRepositoryCollection repositoryCollection,
        IOptions<NlbTestOptions> options, ILogger<ExecutorBase> logger)
    {
        RepositoryCollection = repositoryCollection;
        Options = options.Value;
        Logger = logger;
    }

    public async Task ExecuteAsync(ExecutionContext context, CancellationToken cancel)
    {
        if (cancel.IsCancellationRequested)
            return;

        context.Iteration++;

        var tasks = context.Repositories
            .Select(repositoryName => ExecuteOnRepository(context, repositoryName, cancel)).ToList();

        await Task.WhenAll(tasks);
    }

    protected abstract Task ExecuteOnRepository(ExecutionContext context, string repositoryName, CancellationToken cancel);

    protected async Task<bool> DelayAndCheckCancel(int delayMilliseconds, CancellationToken cancel)
    {
        // wait a bit before starting the next operation
        try
        {
            await Task.Delay(delayMilliseconds, cancel);
        }
        catch (OperationCanceledException)
        {
            return true;
        }

        return cancel.IsCancellationRequested;
    }
}