using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SenseNet.Client;

namespace NLBTester.Executors
{
    internal class ReaderExecutor : ExecutorBase
    {
        // constructor
        public ReaderExecutor(IRepositoryCollection repositoryCollection,
            IOptions<NlbTestOptions> options, 
            ILogger<ReaderExecutor> logger)
            : base(repositoryCollection, options, logger)
        {
        }

        protected override async Task ExecuteOnRepository(ExecutionContext context, string repositoryName, CancellationToken cancel)
        {
            // check if the operation is cancelled
            if (cancel.IsCancellationRequested)
                return;

            var timer = Stopwatch.StartNew();

            Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} started.",
                ExecutorName, context.Id, context.Iteration, repositoryName);

            try
            {
                var repository = await RepositoryCollection.GetRepositoryAsync(repositoryName, cancel);

                // query for files
                var fileContents = await repository.QueryAsync<SenseNet.Client.File>(new QueryContentRequest
                {
                    ContentQuery = "TypeIs:File AND InTree:'/Root/Content'",
                    Select = new[]
                    {
                    "Id", "Name", "Path", "Type", "CreatedBy/Id", "CreatedBy/Name", "CreatedBy/Path",
                    "CreatedBy/Type"
                },
                    Expand = new[] { "CreatedBy" },
                    Top = 100
                }, cancel);

                Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} - " +
                                "File query returned {FileCount} items.",
                    ExecutorName, context.Id, context.Iteration, repositoryName, fileContents.Count);

                // check if the operation is cancelled
                if (cancel.IsCancellationRequested)
                    return;

                // query folder contents
                var folderContents = await repository.QueryAsync<Content>(new QueryContentRequest
                {
                    ContentQuery = "TypeIs:Folder AND InTree:'/Root'",
                    AutoFilters = FilterStatus.Disabled,
                    Select = new[] { "Id", "Name", "Path", "Type" },
                    Top = 100
                }, cancel);

                Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} - " +
                                "Folder query returned {FolderCount} items.",
                    ExecutorName, context.Id, context.Iteration, repositoryName, folderContents.Count);

                // check if the operation is cancelled
                if (cancel.IsCancellationRequested)
                    return;

                // query user contents
                var userContents = await repository.QueryAsync<User>(new QueryContentRequest
                {
                    ContentQuery = "TypeIs:User",
                    Select = new[] { "Id", "Name", "Path", "Type", "LoginName", "Email" },
                    Top = 100
                }, cancel);

                Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} - " +
                                "User query returned {UserCount} items.",
                    ExecutorName, context.Id, context.Iteration, repositoryName, userContents.Count);

            }
            catch (OperationCanceledException)
            {
                // return gracefully, cancel was requested
                return;
            }

            if (await DelayAndCheckCancel(Options.ReadOperations.StepDelay, cancel))
                return;

            timer.Stop();

            Logger.LogTrace("[{ContextId}] {ExecutorName} Iteration {Iteration} on repository {repositoryName} ended. " +
                            "Elapsed time: {ElapsedTime}.",
                ExecutorName, context.Id, context.Iteration, repositoryName, timer.Elapsed);
        }
    }
}
