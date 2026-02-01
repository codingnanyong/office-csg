using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSG.MI.FDW.EF.Migrations
{
    /// <inheritdoc />
    public partial class iot_temperature : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "iot");

            migrationBuilder.CreateTable(
                name: "device",
                schema: "iot",
                columns: table => new
                {
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    mach_id = table.Column<string>(type: "varchar(30)", nullable: false),
                    company_cd = table.Column<string>(type: "varchar(10)", nullable: false),
                    name = table.Column<string>(type: "varchar(30)", nullable: false),
                    descn = table.Column<string>(type: "varchar(200)", nullable: false)
                },
                constraints: table =>
                {
                });

            migrationBuilder.CreateTable(
                name: "machine",
                schema: "iot",
                columns: table => new
                {
                    mach_id = table.Column<string>(type: "varchar(30)", nullable: false),
                    company_cd = table.Column<string>(type: "varchar(10)", nullable: false),
                    mach_kind = table.Column<string>(type: "varchar(20)", nullable: false),
                    mach_name = table.Column<string>(type: "varchar(30)", nullable: false),
                    descn = table.Column<string>(type: "varchar(200)", nullable: false),
                    orgn_cd = table.Column<string>(type: "varchar(10)", nullable: false),
                    loc_cd = table.Column<string>(type: "varchar(10)", nullable: false),
                    line_cd = table.Column<string>(type: "varchar(10)", nullable: false),
                    mline_cd = table.Column<string>(type: "varchar(10)", nullable: false),
                    op_cd = table.Column<string>(type: "varchar(10)", nullable: false)
                },
                constraints: table =>
                {
                });

            migrationBuilder.CreateTable(
                name: "sensor",
                schema: "iot",
                columns: table => new
                {
                    sensor_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    mach_id = table.Column<string>(type: "varchar(30)", nullable: false),
                    company_cd = table.Column<string>(type: "varchar(10)", nullable: false),
                    name = table.Column<string>(type: "varchar(30)", nullable: false),
                    addr = table.Column<string>(type: "varchar(50)", nullable: false),
                    topic = table.Column<string>(type: "varchar(100)", nullable: false),
                    descn = table.Column<string>(type: "varchar(200)", nullable: false)
                },
                constraints: table =>
                {
                });

            migrationBuilder.CreateTable(
                name: "temperature",
                schema: "iot",
                columns: table => new
                {
                    ymd = table.Column<string>(type: "varchar(8)", nullable: false),
                    hmsf = table.Column<string>(type: "varchar(12)", nullable: false),
                    sensor_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    capture_dt = table.Column<DateTime>(type: "timestamp", nullable: false),
                    t1 = table.Column<float>(type: "real", nullable: false),
                    t2 = table.Column<float>(type: "real", nullable: false)
                },
                constraints: table =>
                {
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "device",
                schema: "iot");

            migrationBuilder.DropTable(
                name: "machine",
                schema: "iot");

            migrationBuilder.DropTable(
                name: "sensor",
                schema: "iot");

            migrationBuilder.DropTable(
                name: "temperature",
                schema: "iot");
        }
    }
}
