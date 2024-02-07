using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json.Linq;
using SenseNet.Client;

namespace NLBTester.Executors;

internal class BackupExecutor : ExecutorBase
{
    //protected override bool SingleRepository => true;
    protected bool Executed { get; private set; }
    protected BackupOptions BackupOptions => Options.Backup;
    protected string? ConnectionString { get; set; }

    public BackupExecutor(IRepositoryCollection repositoryCollection, 
        IConfiguration configuration,
        IOptions<NlbTestOptions> options, ILogger<ExecutorBase> logger) : base(repositoryCollection, options, logger)
    {
        ConnectionString = configuration.GetConnectionString("SnCrMsSql");
    }

    protected override async Task ExecuteOnRepository(ExecutionContext context, string repositoryName, CancellationToken cancel)
    {
        if (cancel.IsCancellationRequested || Executed)
            return;

        if (string.IsNullOrEmpty(BackupOptions.IndexTarget))
        {
            Logger.LogWarning("WARNING: Backup executor cannot start, index backup target is missing.");
            return;
        }

        Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} started.",
            context.Id, ExecutorName, context.Iteration, repositoryName);
        
        try
        {
            var repository = await RepositoryCollection.GetRepositoryAsync(repositoryName, cancel);

            // start creating tasks periodically so that the backup/restore process can be observed
            var taskCreationTask = CreateTasksPeriodicallyAsync(context, repository, cancel);

            // wait for at least a couple of tasks to be created
            await Task.Delay(3000, cancel);

            // BACKUP INDEX
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

            // wait for the index backup to finish
            if (BackupOptions.BackupGapSeconds > 0)
            {
                Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName}. " +
                                "POLLING and WAITING for the index backup to finish.",
                    context.Id, ExecutorName, context.Iteration, repositoryName);

                await WaitForIndexBackupAsync(context, repository, cancel);

                Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName}. " +
                                "WAITING {BackupGapDelay} before starting DB backup",
                    context.Id, ExecutorName, context.Iteration, repositoryName,
                    TimeSpan.FromSeconds(BackupOptions.BackupGapSeconds));

                await Task.Delay(BackupOptions.BackupGapSeconds * 1000, cancel);
            }

            // BACKUP DATABASE
            await BackupDatabaseAsync(context, cancel);

            Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} ended.",
                context.Id, ExecutorName, context.Iteration, repositoryName);

            await taskCreationTask;
        }
        catch (OperationCanceledException)
        {
            // return gracefully, cancel was requested
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "[{ContextId}] {ExecutorName} Iteration {Iteration} on repository " +
                                "{repositoryName} threw an ERROR.",
                context.Id, ExecutorName, context.Iteration, repositoryName);
        }
        finally
        {
            Executed = true;
        }
    }

    private async Task WaitForIndexBackupAsync(ExecutionContext context, IRepository repository, CancellationToken cancel)
    {
        while (true)
        {
            if (cancel.IsCancellationRequested)
                return;

            await Task.Delay(1000, cancel);

            dynamic backupResult = await repository.InvokeFunctionAsync<JObject>(new OperationRequest
            {
                Path = "/Root",
                OperationName = "QueryIndexBackup"
            }, cancel);

            var state = (string)backupResult.State;

            Logger.LogTrace("[{ContextId}] {ExecutorName} BackupIndex state: {BackupState} ",
                context.Id, ExecutorName, state);

            switch (state)
            {
                case "Started":
                case "Executing":
                    continue;
                default:
                    // all other states mean the backup has finished, canceled or failed
                    return;
            }
        }
    }
    
    private const string BackupQuery = "BACKUP DATABASE @DatabaseName TO DISK = @BackupFilePath WITH NOFORMAT, NOINIT, SKIP, NOREWIND, NOUNLOAD";

    private async Task BackupDatabaseAsync(ExecutionContext context, CancellationToken cancel)
    {
        if (string.IsNullOrEmpty(ConnectionString) || string.IsNullOrEmpty(BackupOptions.DatabaseTarget))
        {
            Logger.LogWarning("[{ContextId}] {ExecutorName} Cannot create database backup, SQL connection string, " +
                            "db name or target path is missing.",
                context.Id, ExecutorName);
            return;
        }
        
        try
        {
            var builder = new SqlConnectionStringBuilder(ConnectionString);
            var databaseName = builder.InitialCatalog;

            Logger.LogTrace("[{ContextId}] {ExecutorName} Starting DB BACKUP on database {DatabaseName}. Target: {DatabaseTarget}",
                context.Id, ExecutorName, databaseName, BackupOptions.DatabaseTarget);

            await using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync(cancel);

            await using var command = new SqlCommand(BackupQuery, connection);

            command.Parameters.AddWithValue("@DatabaseName", databaseName);
            command.Parameters.AddWithValue("@BackupFilePath", BackupOptions.DatabaseTarget);

            command.CommandTimeout = 60 * 5;

            await command.ExecuteNonQueryAsync(cancel);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "[{ContextId}] {ExecutorName} ERROR when creating DB backup.", context.Id, ExecutorName);
            return;
        }

        Logger.LogTrace("[{ContextId}] {ExecutorName} DB BACKUP finished successfully.", context.Id, ExecutorName);
    }

    private async Task CreateTasksPeriodicallyAsync(ExecutionContext context, IRepository repository, CancellationToken cancel)
    {
        var taskCount = 0;

        while (!cancel.IsCancellationRequested)
        {
            try
            {
                // create a new task
                var taskName = $"LogTask-{DateTime.UtcNow:yyyy-MM-dd-HH-mm-ss}-{taskCount++}";
                var taskContent = repository.CreateContent(TestContents.TaskListPath, "Task", taskName);
                taskContent["DisplayName"] = taskName;

                await taskContent.SaveAsync(cancel);

                Logger.LogTrace("[{ContextId}] {ExecutorName} Marker task created: {LogTaskName} ",
                    context.Id, ExecutorName, taskName);

                await Task.Delay(1000, cancel);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error when creating a new log task.");
                return;
            }
        }
    }
}