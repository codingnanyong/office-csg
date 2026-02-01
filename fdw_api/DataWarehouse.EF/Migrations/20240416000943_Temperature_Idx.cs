using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSG.MI.FDW.EF.Migrations
{
    /// <inheritdoc />
    public partial class Temperature_Idx : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateIndex(
                name: "idx_multi_temperature",
                schema: "iot",
                table: "temperature",
                columns: new[] { "device_id", "capture_dt" });

            migrationBuilder.CreateIndex(
                name: "idx_temperature",
                schema: "iot",
                table: "temperature",
                column: "capture_dt");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "idx_multi_temperature",
                schema: "iot",
                table: "temperature");

            migrationBuilder.DropIndex(
                name: "idx_temperature",
                schema: "iot",
                table: "temperature");
        }
    }
}
