using System;
using CSG.MI.TrMontrgSrv.Model.Json;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

namespace CSG.MI.TrMontrgSrv.EF.Migrations
{
    public partial class initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "public");

            migrationBuilder.CreateTable(
                name: "device",
                schema: "public",
                columns: table => new
                {
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    loc_id = table.Column<string>(type: "varchar(10)", nullable: false),
                    plant_id = table.Column<string>(type: "varchar(10)", nullable: false),
                    name = table.Column<string>(type: "varchar(30)", nullable: true),
                    desn = table.Column<string>(type: "varchar(200)", nullable: true),
                    root_path = table.Column<string>(type: "varchar(255)", nullable: false),
                    ord = table.Column<int>(type: "integer", nullable: true),
                    upd_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("device_pkey", x => x.device_id);
                });

            migrationBuilder.CreateTable(
                name: "box",
                schema: "public",
                columns: table => new
                {
                    ymd = table.Column<string>(type: "varchar(8)", nullable: false),
                    hms = table.Column<string>(type: "varchar(6)", nullable: false),
                    box_id = table.Column<int>(type: "integer", nullable: false),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    capture_dt = table.Column<DateTime>(type: "timestamp", nullable: false),
                    t_avg = table.Column<float>(type: "real", nullable: false),
                    t_max = table.Column<float>(type: "real", nullable: false),
                    t_min = table.Column<float>(type: "real", nullable: false),
                    t_diff = table.Column<float>(type: "real", nullable: false),
                    t_90th = table.Column<float>(type: "real", nullable: false),
                    t_max_x = table.Column<int>(type: "integer", nullable: false),
                    t_max_y = table.Column<int>(type: "integer", nullable: false),
                    p1_x = table.Column<int>(type: "integer", nullable: false),
                    p1_y = table.Column<int>(type: "integer", nullable: false),
                    p2_x = table.Column<int>(type: "integer", nullable: false),
                    p2_y = table.Column<int>(type: "integer", nullable: false),
                    upd_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("box_pkey", x => new { x.ymd, x.hms, x.box_id, x.device_id });
                    table.ForeignKey(
                        name: "box_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "cfg",
                schema: "public",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    ymd = table.Column<string>(type: "varchar(8)", nullable: false),
                    hms = table.Column<string>(type: "varchar(6)", nullable: false),
                    capture_dt = table.Column<DateTime>(type: "timestamp", nullable: false),
                    cfg_file = table.Column<ConfigJson>(type: "jsonb", nullable: false),
                    upd_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("cfg_pkey", x => x.id);
                    table.ForeignKey(
                        name: "cfg_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "frame",
                schema: "public",
                columns: table => new
                {
                    ymd = table.Column<string>(type: "varchar(8)", nullable: false),
                    hms = table.Column<string>(type: "varchar(6)", nullable: false),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    capture_dt = table.Column<DateTime>(type: "timestamp", nullable: false),
                    t_avg = table.Column<float>(type: "real", nullable: false),
                    t_max = table.Column<float>(type: "real", nullable: false),
                    t_min = table.Column<float>(type: "real", nullable: false),
                    t_diff = table.Column<float>(type: "real", nullable: false),
                    t_90th = table.Column<float>(type: "real", nullable: false),
                    t_max_x = table.Column<int>(type: "integer", nullable: false),
                    t_max_y = table.Column<int>(type: "integer", nullable: false),
                    upd_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("frame_pkey", x => new { x.ymd, x.hms, x.device_id });
                    table.ForeignKey(
                        name: "frame_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "medium",
                schema: "public",
                columns: table => new
                {
                    ymd = table.Column<string>(type: "varchar(8)", nullable: false),
                    hms = table.Column<string>(type: "varchar(6)", nullable: false),
                    medium_type = table.Column<string>(type: "varchar(10)", nullable: false),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    capture_dt = table.Column<DateTime>(type: "timestamp", nullable: false),
                    file_name = table.Column<string>(type: "varchar(255)", nullable: false),
                    file_type = table.Column<string>(type: "varchar(20)", nullable: false),
                    file_size = table.Column<long>(type: "bigint", nullable: false),
                    file_content = table.Column<byte[]>(type: "bytea", nullable: false),
                    upd_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("medium_pkey", x => new { x.ymd, x.hms, x.medium_type, x.device_id });
                    table.ForeignKey(
                        name: "medium_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "roi",
                schema: "public",
                columns: table => new
                {
                    ymd = table.Column<string>(type: "varchar(8)", nullable: false),
                    hms = table.Column<string>(type: "varchar(6)", nullable: false),
                    roi_id = table.Column<int>(type: "integer", nullable: false),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false),
                    capture_dt = table.Column<DateTime>(type: "timestamp", nullable: false),
                    t_avg = table.Column<float>(type: "real", nullable: false),
                    t_max = table.Column<float>(type: "real", nullable: false),
                    t_min = table.Column<float>(type: "real", nullable: false),
                    t_diff = table.Column<float>(type: "real", nullable: false),
                    t_90th = table.Column<float>(type: "real", nullable: false),
                    t_max_x = table.Column<int>(type: "integer", nullable: false),
                    t_max_y = table.Column<int>(type: "integer", nullable: false),
                    p1_x = table.Column<int>(type: "integer", nullable: false),
                    p1_y = table.Column<int>(type: "integer", nullable: false),
                    p2_x = table.Column<int>(type: "integer", nullable: false),
                    p2_y = table.Column<int>(type: "integer", nullable: false),
                    upd_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("roi_pkey", x => new { x.ymd, x.hms, x.roi_id, x.device_id });
                    table.ForeignKey(
                        name: "roi_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "box_device_id_idx",
                schema: "public",
                table: "box",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "cfg_device_id_idx",
                schema: "public",
                table: "cfg",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "cfg_ymd_hms_idx",
                schema: "public",
                table: "cfg",
                columns: new[] { "ymd", "hms" });

            migrationBuilder.CreateIndex(
                name: "ix_ymd_hms",
                schema: "public",
                table: "cfg",
                columns: new[] { "ymd", "hms" });

            migrationBuilder.CreateIndex(
                name: "device_loc_id_plant_id_idx",
                schema: "public",
                table: "device",
                columns: new[] { "loc_id", "plant_id" });

            migrationBuilder.CreateIndex(
                name: "frame_device_id_idx",
                schema: "public",
                table: "frame",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "medium_device_id_idx",
                schema: "public",
                table: "medium",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "roi_device_id_idx",
                schema: "public",
                table: "roi",
                column: "device_id");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "box",
                schema: "public");

            migrationBuilder.DropTable(
                name: "cfg",
                schema: "public");

            migrationBuilder.DropTable(
                name: "frame",
                schema: "public");

            migrationBuilder.DropTable(
                name: "medium",
                schema: "public");

            migrationBuilder.DropTable(
                name: "roi",
                schema: "public");

            migrationBuilder.DropTable(
                name: "device",
                schema: "public");
        }
    }
}
