namespace NLBTester;

internal class ExecutionContext
{
    public string Id { get; } = Guid.NewGuid().ToString();
    public int Iteration { get; set; }
    public string[] Repositories { get; set; } = Array.Empty<string>();
}