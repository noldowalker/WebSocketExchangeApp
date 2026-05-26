using Aggregator.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Aggregator.Infrastructure.Migrations;

[DbContext(typeof(AggregatorDbContext))]
[Migration("202605260001_TradeTickQuoteFields")]
public sealed class TradeTickQuoteFields : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.RenameColumn(
            name: "value",
            table: "trade_ticks",
            newName: "price");

        migrationBuilder.AddColumn<string>(
            name: "ticker",
            table: "trade_ticks",
            type: "character varying(32)",
            maxLength: 32,
            nullable: false,
            defaultValue: "");

        migrationBuilder.AddColumn<decimal>(
            name: "volume",
            table: "trade_ticks",
            type: "numeric(18,8)",
            precision: 18,
            scale: 8,
            nullable: false,
            defaultValue: 0m);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(
            name: "ticker",
            table: "trade_ticks");

        migrationBuilder.DropColumn(
            name: "volume",
            table: "trade_ticks");

        migrationBuilder.RenameColumn(
            name: "price",
            table: "trade_ticks",
            newName: "value");
    }
}
