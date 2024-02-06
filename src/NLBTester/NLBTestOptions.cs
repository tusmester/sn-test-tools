namespace NLBTester;

internal class NlbTestOptions
{
    public OperationOptions WriteOperations { get; set; } = new();
    public OperationOptions ReadOperations { get; set; } = new();
    public BackupOptions Backup { get; set; } = new();
}

internal class OperationOptions
{
    public int ThreadCount { get; set; } = 1;

    /// <summary>
    /// Number of seconds to wait before starting the first iteration. Default: 0.
    /// </summary>
    public int InitialDelaySeconds { get; set; } = 0;
    /// <summary>
    /// Number of milliseconds to wait between steps. Default: 200.
    /// </summary>
    public int StepDelay { get; set; } = 200;
}

internal class BackupOptions : OperationOptions
{
    public string? IndexTarget { get; set; }
    public string? DatabaseTarget { get; set; }
    /// <summary>
    /// How much time should pass between index and db backup operations. Default: 15 seconds.
    /// </summary>
    public int BackupGapSeconds { get; set; } = 15;
}