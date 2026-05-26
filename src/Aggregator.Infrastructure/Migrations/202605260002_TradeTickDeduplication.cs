using Aggregator.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Aggregator.Infrastructure.Migrations;

[DbContext(typeof(AggregatorDbContext))]
[Migration("202605260002_TradeTickDeduplication")]
public sealed class TradeTickDeduplication : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql("""
            delete from trade_ticks
            where id in (
                select id
                from (
                    select
                        id,
                        row_number() over (
                            partition by source, ticker, price, volume, timestamp_utc
                            order by id
                        ) as row_number_value
                    from trade_ticks
                ) duplicates
                where duplicates.row_number_value > 1
            );
            """);

        migrationBuilder.CreateIndex(
            name: "ux_trade_ticks_source_ticker_price_volume_timestamp",
            table: "trade_ticks",
            columns: new[] { "source", "ticker", "price", "volume", "timestamp_utc" },
            unique: true);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "ux_trade_ticks_source_ticker_price_volume_timestamp",
            table: "trade_ticks");
    }
}
