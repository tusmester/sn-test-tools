using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using SenseNet.Client;

namespace NLBTester.Executors;

internal class BackupExecutor : ExecutorBase
{
    //protected override bool SingleRepository => true;
    protected bool Executed { get; private set; } = false;
    protected BackupOptions BackupOptions => Options.Backup;

    public BackupExecutor(IRepositoryCollection repositoryCollection, IOptions<NlbTestOptions> options, ILogger<ExecutorBase> logger) : base(repositoryCollection, options, logger)
    {
    }

    protected override async Task ExecuteOnRepository(ExecutionContext context, string repositoryName, CancellationToken cancel)
    {
        //TODO: if the backup has already been executed, monitor the process and log the progress.

        if (cancel.IsCancellationRequested || Executed)
            return;

        Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} started.",
            context.Id, ExecutorName, context.Iteration, repositoryName);

        try
        {
            var repository = await RepositoryCollection.GetRepositoryAsync(repositoryName, cancel);

            dynamic backupResult = await repository.InvokeActionAsync<JObject>(new OperationRequest
            {
                Path = "/Root",
                OperationName = "BackupIndex",
                PostData = new
                {
                    target = BackupOptions.IndexTarget
                }
            }, cancel);

            var state = (string)backupResult.State;

            Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository " +
                            "{repositoryName} - BackupIndex state: {State}.",
                context.Id, ExecutorName, context.Iteration, repositoryName, state.ToUpper());
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "[{ContextId}] {ExecutorName} Iteration {Iteration} on repository " +
                                "{repositoryName} threw an ERROR.",
                context.Id, ExecutorName, context.Iteration, repositoryName);

            return;
        }
        finally
        {
            Executed = true;
        }

        //TODO: monitor the backup process and log the progress.
        //TODO: initiate a SQL db backup if requested.

        Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} ended.",
            context.Id, ExecutorName, context.Iteration, repositoryName);
    }
}