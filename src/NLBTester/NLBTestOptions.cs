namespace NLBTester;

internal class NlbTestOptions
{
    public int ThreadCount { get; set; } = 1;
    /// <summary>
    /// Number of milliseconds to wait between steps. Default: 200.
    /// </summary>
    public int StepDelay { get; set; } = 200;
}