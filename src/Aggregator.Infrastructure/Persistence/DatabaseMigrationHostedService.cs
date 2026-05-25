using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Aggregator.Infrastructure.Persistence;

public sealed class DatabaseMigrationHostedService : IHostedService
{
    private readonly IDbContextFactory<AggregatorDbContext> _dbContextFactory;
    private readonly ILogger<DatabaseMigrationHostedService> _logger;

    public DatabaseMigrationHostedService(
        IDbContextFactory<AggregatorDbContext> dbContextFactory,
        ILogger<DatabaseMigrationHostedService> logger)
    {
        _dbContextFactory = dbContextFactory;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var dbContext = await _dbContextFactory.CreateDbContextAsync(cancellationToken);
        _logger.LogInformation("Applying EF Core migrations...");
        await dbContext.Database.MigrateAsync(cancellationToken);
        _logger.LogInformation("EF Core migrations applied.");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
