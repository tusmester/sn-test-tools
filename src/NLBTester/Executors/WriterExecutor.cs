using SenseNet.Client;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using File = SenseNet.Client.File;
using OperationCanceledException = System.OperationCanceledException;

namespace NLBTester.Executors;

internal class WriterExecutor : ExecutorBase
{
    public WriterExecutor(IRepositoryCollection repositoryCollection,
        IOptions<NlbTestOptions> options,
        ILogger<WriterExecutor> logger) : base(repositoryCollection, options, logger)
    {
    }

    protected override async Task ExecuteOnRepository(ExecutionContext context, string repositoryName, CancellationToken cancel)
    {
        if (cancel.IsCancellationRequested)
            return;

        var filesFolderPath = Path.Combine(AppContext.BaseDirectory, "files");
        var filePaths = Directory.GetFiles(filesFolderPath);

        var timer = Stopwatch.StartNew();

        Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} started.",
            context.Id, ExecutorName, context.Iteration, repositoryName);

        var repository = await RepositoryCollection.GetRepositoryAsync(repositoryName, cancel);
        var parentPath = RepositoryPath.Combine(TestContents.DocumentLibraryPath, repositoryName);

        // UPLOAD start
        var uploadTasks = filePaths
            .Select(filePath => UploadFile(repository, parentPath, filePath, cancel))
            .ToList();

        await Task.WhenAll(uploadTasks);

        if (cancel.IsCancellationRequested)
            return;

        if (await DelayAndCheckCancel(Options.WriteOperations.StepDelay, cancel))
            return;

        // get uploaded file ids
        var uploadedFileIds = uploadTasks
            .Where(uploadTask => uploadTask.Result.Id != 0)
            .Select(uploadTask => uploadTask.Result.Id).ToArray();

        // check uploaded file count
        try
        {
            var fileContents = await repository.QueryAsync<File>(new QueryContentRequest
            {
                ContentQuery = $"TypeIs:File AND Id:({string.Join(' ', uploadedFileIds)})",
                Select = new[] { "Id", "Name", "Path", "Type" },
            }, cancel);

            if (fileContents.Count != uploadedFileIds.Length)
            {
                Logger.LogWarning("[{ContextId}] File count mismatch in repository " +
                                  "{repositoryName}: {QueryContentCount} (queried) / {UploadedContentCount} (uploaded)",
                    context.Id, repositoryName, fileContents.Count, uploadedFileIds.Length);
            }
        }
        catch (OperationCanceledException)
        {
            // return gracefully, cancel was requested
            return;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "[{ContextId}] ERROR while querying uploaded files in repository {RepositoryName}.",
                context.Id, repositoryName);
        }

        if (cancel.IsCancellationRequested)
            return;

        if (await DelayAndCheckCancel(Options.WriteOperations.StepDelay, cancel))
            return;

        // DELETE start
        Logger.LogTrace("[{ContextId}] Deleting uploaded files from repository {repositoryName} in iteration {Iteration}.",
            context.Id, repositoryName, context.Iteration);

        var deleteTasks = uploadedFileIds
            .Select(async (fileId) =>
            {
                try
                {
                    await repository.DeleteContentAsync(fileId, true, cancel);
                }
                catch (OperationCanceledException)
                {
                    // do nothing, cancel was requested
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, "[{ContextId}] ERROR when deleting file {FileId} in repository {RepositoryName}.",
                        context.Id, fileId, repositoryName);
                }
            })
            .ToList();

        await Task.WhenAll(deleteTasks);

        timer.Stop();

        Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} ended. " +
                        "Elapsed time: {ElapsedTime}.",
            context.Id, ExecutorName, context.Iteration, repositoryName, timer.Elapsed);
    }
        
    private async Task<UploadResult> UploadFile(IRepository repo, string parentPath, string filePath, CancellationToken cancel)
    {
        if (cancel.IsCancellationRequested)
            return new UploadResult();

        var contentName = Path.GetFileNameWithoutExtension(filePath) + "-" + Guid.NewGuid() + Path.GetExtension(filePath);

        try
        {
            await using var fileStream = System.IO.File.OpenRead(filePath);

            var uploadResult = await repo.UploadAsync(new UploadRequest
            {
                ContentName = contentName,
                ParentPath = parentPath
            }, fileStream, cancel);

            return uploadResult;
        }
        catch (OperationCanceledException)
        {
            // do nothing, cancel was requested
        }
        catch(Exception ex)
        {
            Logger.LogError(ex, "Error while uploading file {FilePath} to repository {RepositoryName}.",
                filePath, repo.Server.Url);
        }

        return new UploadResult();
    }
}