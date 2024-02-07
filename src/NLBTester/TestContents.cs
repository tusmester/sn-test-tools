using SenseNet.Client;

namespace NLBTester;

internal static class TestContents
{
    public const string WorkspacePath = "/Root/Content/nlbtest";
    public static string DocumentLibraryPath => RepositoryPath.Combine(WorkspacePath, "doclib");
    public static string MemoListPath => RepositoryPath.Combine(WorkspacePath, "memolist");
}