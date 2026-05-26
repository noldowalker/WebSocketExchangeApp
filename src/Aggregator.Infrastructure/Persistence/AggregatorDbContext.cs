using Microsoft.EntityFrameworkCore;

namespace Aggregator.Infrastructure.Persistence;

public sealed class AggregatorDbContext : DbContext
{
    public AggregatorDbContext(DbContextOptions<AggregatorDbContext> options) : base(options)
    {
    }

    public DbSet<TradeTickEntity> TradeTicks => Set<TradeTickEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TradeTickEntity>(entity =>
        {
            entity.ToTable("trade_ticks");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Source).HasColumnName("source").HasMaxLength(64).IsRequired();
            entity.Property(x => x.Ticker).HasColumnName("ticker").HasMaxLength(32).IsRequired();
            entity.Property(x => x.Price).HasColumnName("price").HasPrecision(18, 8);
            entity.Property(x => x.Volume).HasColumnName("volume").HasPrecision(18, 8);
            entity.Property(x => x.TimestampUtc).HasColumnName("timestamp_utc").IsRequired();
            entity.HasIndex(x => new { x.Source, x.Ticker, x.Price, x.Volume, x.TimestampUtc })
                .IsUnique()
                .HasDatabaseName("ux_trade_ticks_source_ticker_price_volume_timestamp");
        });
    }
}
