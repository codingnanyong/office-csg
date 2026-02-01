using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSG.MI.FDW.EF.Migrations
{
    /// <inheritdoc />
    public partial class Coordinate_histroies : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "worksheets_coordinate_history",
                schema: "services",
                columns: table => new
                {
                    wsno = table.Column<string>(type: "varchar(20)", nullable: false),
                    tagid = table.Column<string>(type: "varchar(12)", nullable: true),
                    tag_type = table.Column<string>(type: "varchar(1)", nullable: true),
                    opcd = table.Column<string>(type: "varchar(10)", nullable: true),
                    status = table.Column<string>(type: "varchar(1)", nullable: true),
                    zone = table.Column<string>(type: "varchar(12)", nullable: true),
                    floor = table.Column<string>(type: "varchar(30)", nullable: true),
                    now = table.Column<DateTime>(type: "timestamp", nullable: false),
                    next = table.Column<DateTime>(type: "timestamp", nullable: false),
                    diff = table.Column<TimeSpan>(type: "interval", nullable: true)
                },
                constraints: table =>
                {
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "worksheets_coordinate_history",
                schema: "services");
        }
    }
}
