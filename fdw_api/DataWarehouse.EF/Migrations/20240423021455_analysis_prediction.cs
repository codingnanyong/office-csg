using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSG.MI.FDW.EF.Migrations
{
    /// <inheritdoc />
    public partial class analysis_prediction : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "dev_style_prediction",
                schema: "services",
                columns: table => new
                {
                    dev_style_number = table.Column<string>(type: "varchar(30)", nullable: false),
                    tag_type = table.Column<string>(type: "varchar(1)", nullable: false),
                    opcd = table.Column<string>(type: "varchar(10)", nullable: false),
                    time = table.Column<DateTime>(type: "timestamp", nullable: false)
                },
                constraints: table =>
                {
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "dev_style_prediction",
                schema: "services");
        }
    }
}
