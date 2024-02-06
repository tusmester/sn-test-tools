namespace NLBTester;

internal class OperationResult
{
    public TimeSpan Elapsed { get; set; }
    public int WriteIterationCount { get; set; }
    public int ReadIterationCount { get; set; }
}