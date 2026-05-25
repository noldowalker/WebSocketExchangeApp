using Aggregator.Infrastructure.Persistence;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Aggregator.Infrastructure.Migrations;

[DbContext(typeof(AggregatorDbContext))]
[Migration("202605250001_InitialTradeTicks")]
public sealed class InitialTradeTicks : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "trade_ticks",
            columns: table => new
            {
                id = table.Column<long>(type: "bigint", nullable: false)
                    .Annotation("Npgsql:ValueGenerationStrategy", Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                source = table.Column<string>(type: "character varying(64)", maxLength: 64, nullable: false),
                value = table.Column<decimal>(type: "numeric(18,8)", precision: 18, scale: 8, nullable: false),
                timestamp_utc = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
            },
            constraints: table => { table.PrimaryKey("PK_trade_ticks", x => x.id); });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "trade_ticks");
    }
}
