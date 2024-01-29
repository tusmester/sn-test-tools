using SenseNet.Client;
using System.Diagnostics;
using Microsoft.Extensions.Options;
using File = SenseNet.Client.File;
using OperationCanceledException = System.OperationCanceledException;

namespace NLBTester
{
    internal class TestExecutor
    {
        public const string TestContainerPath = "/Root/Content/nlbtestlib";

        private readonly IRepositoryCollection _repositoryCollection;
        private readonly NlbTestOptions _options;
        private int _iteration;
        private string ThreadIdentifier { get; } = Guid.NewGuid().ToString();

        public TestExecutor(IRepositoryCollection repositoryCollection, IOptions<NlbTestOptions> options)
        {
            _repositoryCollection = repositoryCollection;
            _options = options.Value;
        }

        public async Task ExecuteAsync(string[] repositories, CancellationToken cancel)
        {
            if (cancel.IsCancellationRequested)
                return;

            _iteration++;

            var filesFolderPath = Path.Combine(AppContext.BaseDirectory, "files");
            var filePaths = Directory.GetFiles(filesFolderPath);

            var tasks = repositories.Select(repositoryName => ExecuteOnRepository(repositoryName, filePaths, cancel)).ToList();

            await Task.WhenAll(tasks);
        }

        private async Task ExecuteOnRepository(string repositoryName, string[] filePaths, CancellationToken cancel)
        {
            if (cancel.IsCancellationRequested)
                return;

            var time = Stopwatch.StartNew();
            ConsoleWriteLine($"Iteration {_iteration} on {repositoryName} started.");

            var repository = await _repositoryCollection.GetRepositoryAsync(repositoryName, cancel);
            var parentPath = RepositoryPath.Combine(TestContainerPath, repositoryName);

            // UPLOAD start
            var uploadTasks = filePaths
                .Select(filePath => UploadFile(repository, parentPath, filePath, string.Empty, cancel))
                .ToList();

            await Task.WhenAll(uploadTasks);

            if (cancel.IsCancellationRequested)
                return;

            if (await DelayAndCheckCancel(_options.StepDelay, cancel))
                return;

            // get uploaded file ids
            var fileIds = uploadTasks
                .Where(uploadTask => uploadTask.Result.Id != 0)
                .Select(uploadTask => uploadTask.Result.Id).ToArray();

            // check uploaded file count
            try
            {
                var fileContents = await repository.QueryAsync<File>(new QueryContentRequest
                {
                    ContentQuery = $"TypeIs:File AND Id:({string.Join(' ', fileIds)})",
                    Select = new[] { "Id", "Name", "Path", "Type" },
                }, cancel);

                if (fileContents.Count != fileIds.Length)
                    ConsoleWriteLine($"File count mismatch in repository {repositoryName}: {fileContents.Count} != {fileIds.Length}");
            }
            catch (OperationCanceledException)
            {
                // return gracefully, cancel was requested
                return;
            }

            if (cancel.IsCancellationRequested)
                return;

            if (await DelayAndCheckCancel(_options.StepDelay, cancel))
                return;

            // DELETE start
            var deleteTasks = fileIds
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
                })
                .ToList();

            await Task.WhenAll(deleteTasks);

            ConsoleWriteLine($"Iteration {_iteration} on {repositoryName} ended. Elapsed time: {time.Elapsed}");
        }

        private static async Task<bool> DelayAndCheckCancel(int delayMilliseconds, CancellationToken cancel)
        {
            // wait a bit before starting the next operation
            try
            {
                await Task.Delay(delayMilliseconds, cancel);
            }
            catch (TaskCanceledException)
            {
                return true;
            }

            return cancel.IsCancellationRequested;
        }

        private async Task<UploadResult> UploadFile(IRepository repo, string parentPath, string filePath, string taskName, CancellationToken cancel)
        {
            if (cancel.IsCancellationRequested)
                return new UploadResult();

            var time = Stopwatch.StartNew();
            await using var fileStream = System.IO.File.OpenRead(filePath);

            if (cancel.IsCancellationRequested)
                return new UploadResult();

            var contentName = Path.GetFileNameWithoutExtension(filePath) + "-" + Guid.NewGuid() + Path.GetExtension(filePath);

            //ConsoleWriteLine($"{taskName} start.");

            try
            {
                var uploadResult = await repo.UploadAsync(new UploadRequest
                {
                    ContentName = contentName,
                    ParentPath = parentPath
                }, fileStream, cancel);

                //ConsoleWriteLine($"{taskName} end. File {uploadResult.Name} uploaded. Time: {time.Elapsed}");

                return uploadResult;
            }
            catch (OperationCanceledException)
            {
                // do nothing, cancel was requested
            }

            return new UploadResult();
        }

        private void ConsoleWriteLine(string message)
        {
            Console.WriteLine($"[{ThreadIdentifier}] {message}");
        }
    }
}
