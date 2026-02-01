using System;
using CSG.MI.TrMontrgSrv.Model.Json;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

namespace CSG.MI.TrMontrgSrv.EF.Migrations
{
    public partial class AddCtrlTables : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "roi",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                comment: "Last updated",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp");

            migrationBuilder.AlterColumn<float>(
                name: "t_min",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                comment: "Min. temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_y",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                comment: "Y-coordinate value of min. temperature",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_x",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                comment: "X-coordinate value of max. temperature",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<float>(
                name: "t_max",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                comment: "Max. temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_diff",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                comment: "Max. - min temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_avg",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                comment: "Average temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_90th",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                comment: "90 percentile temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<int>(
                name: "p2_y",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                comment: "Y-coordinate value of bottom-right point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "p2_x",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                comment: "X-coordinate value of bottom-right point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "p1_y",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                comment: "Y-coordinate value of top-left point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "p1_x",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                comment: "X-coordinate value of top-left point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "roi",
                type: "timestamp",
                nullable: false,
                comment: "Data captured date time",
                oldClrType: typeof(DateTime),
                oldType: "timestamp");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "roi",
                type: "varchar(20)",
                nullable: false,
                comment: "Device ID",
                oldClrType: typeof(string),
                oldType: "varchar(20)");

            migrationBuilder.AlterColumn<int>(
                name: "roi_id",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                comment: "ROI ID",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "roi",
                type: "varchar(6)",
                nullable: false,
                comment: "Data captured time",
                oldClrType: typeof(string),
                oldType: "varchar(6)");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "roi",
                type: "varchar(8)",
                nullable: false,
                comment: "Data captured date",
                oldClrType: typeof(string),
                oldType: "varchar(8)");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "medium",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                comment: "Last updated",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp");

            migrationBuilder.AlterColumn<string>(
                name: "file_type",
                schema: "public",
                table: "medium",
                type: "varchar(20)",
                nullable: false,
                comment: "File type",
                oldClrType: typeof(string),
                oldType: "varchar(20)");

            migrationBuilder.AlterColumn<long>(
                name: "file_size",
                schema: "public",
                table: "medium",
                type: "bigint",
                nullable: false,
                comment: "File size in bytes",
                oldClrType: typeof(long),
                oldType: "bigint");

            migrationBuilder.AlterColumn<string>(
                name: "file_name",
                schema: "public",
                table: "medium",
                type: "varchar(255)",
                nullable: false,
                comment: "File name",
                oldClrType: typeof(string),
                oldType: "varchar(255)");

            migrationBuilder.AlterColumn<byte[]>(
                name: "file_content",
                schema: "public",
                table: "medium",
                type: "bytea",
                nullable: false,
                comment: "File content",
                oldClrType: typeof(byte[]),
                oldType: "bytea");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "medium",
                type: "timestamp",
                nullable: false,
                comment: "Data captured date time",
                oldClrType: typeof(DateTime),
                oldType: "timestamp");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "medium",
                type: "varchar(20)",
                nullable: false,
                comment: "Device ID",
                oldClrType: typeof(string),
                oldType: "varchar(20)");

            migrationBuilder.AlterColumn<string>(
                name: "medium_type",
                schema: "public",
                table: "medium",
                type: "varchar(10)",
                nullable: false,
                comment: "Medium type",
                oldClrType: typeof(string),
                oldType: "varchar(10)");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "medium",
                type: "varchar(6)",
                nullable: false,
                comment: "Data captured time",
                oldClrType: typeof(string),
                oldType: "varchar(6)");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "medium",
                type: "varchar(8)",
                nullable: false,
                comment: "Data captured date",
                oldClrType: typeof(string),
                oldType: "varchar(8)");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "frame",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                comment: "Last updated",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp");

            migrationBuilder.AlterColumn<float>(
                name: "t_min",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                comment: "Min. temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_y",
                schema: "public",
                table: "frame",
                type: "integer",
                nullable: false,
                comment: "Y-coordinate value of min. temperature",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_x",
                schema: "public",
                table: "frame",
                type: "integer",
                nullable: false,
                comment: "X-coordinate value of max. temperature",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<float>(
                name: "t_max",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                comment: "Max. temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_diff",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                comment: "Max. - min temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_avg",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                comment: "Average temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_90th",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                comment: "90 percentile temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "frame",
                type: "timestamp",
                nullable: false,
                comment: "Data captured date time",
                oldClrType: typeof(DateTime),
                oldType: "timestamp");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "frame",
                type: "varchar(20)",
                nullable: false,
                comment: "Device ID",
                oldClrType: typeof(string),
                oldType: "varchar(20)");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "frame",
                type: "varchar(6)",
                nullable: false,
                comment: "Data captured time",
                oldClrType: typeof(string),
                oldType: "varchar(6)");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "frame",
                type: "varchar(8)",
                nullable: false,
                comment: "Data captured date",
                oldClrType: typeof(string),
                oldType: "varchar(8)");

            migrationBuilder.AddColumn<int>(
                name: "t_min_x",
                schema: "public",
                table: "frame",
                type: "integer",
                nullable: true,
                comment: "X-coordinate value of min. temperature");

            migrationBuilder.AddColumn<int>(
                name: "t_min_y",
                schema: "public",
                table: "frame",
                type: "integer",
                nullable: true,
                comment: "Y-coordinate value of min. temperature");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "device",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                comment: "Last updated",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp");

            migrationBuilder.AlterColumn<string>(
                name: "root_path",
                schema: "public",
                table: "device",
                type: "varchar(255)",
                nullable: false,
                comment: "Root path of data directory",
                oldClrType: typeof(string),
                oldType: "varchar(255)");

            migrationBuilder.AlterColumn<string>(
                name: "plant_id",
                schema: "public",
                table: "device",
                type: "varchar(10)",
                nullable: false,
                comment: "Plant ID",
                oldClrType: typeof(string),
                oldType: "varchar(10)");

            migrationBuilder.AlterColumn<int>(
                name: "ord",
                schema: "public",
                table: "device",
                type: "integer",
                nullable: true,
                comment: "Order by",
                oldClrType: typeof(int),
                oldType: "integer",
                oldNullable: true);

            migrationBuilder.AlterColumn<string>(
                name: "name",
                schema: "public",
                table: "device",
                type: "varchar(30)",
                nullable: true,
                comment: "Name of device",
                oldClrType: typeof(string),
                oldType: "varchar(30)",
                oldNullable: true);

            migrationBuilder.AlterColumn<string>(
                name: "loc_id",
                schema: "public",
                table: "device",
                type: "varchar(10)",
                nullable: false,
                comment: "Location ID",
                oldClrType: typeof(string),
                oldType: "varchar(10)");

            migrationBuilder.AlterColumn<string>(
                name: "desn",
                schema: "public",
                table: "device",
                type: "varchar(200)",
                nullable: true,
                comment: "Description",
                oldClrType: typeof(string),
                oldType: "varchar(200)",
                oldNullable: true);

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "device",
                type: "varchar(20)",
                nullable: false,
                comment: "Device ID",
                oldClrType: typeof(string),
                oldType: "varchar(20)");

            migrationBuilder.AddColumn<int>(
                name: "api_port",
                schema: "public",
                table: "device",
                type: "integer",
                nullable: true,
                comment: "Port number of RESTful API service");

            migrationBuilder.AddColumn<string>(
                name: "ip_addr",
                schema: "public",
                table: "device",
                type: "varchar(45)",
                nullable: true,
                comment: "IP address");

            migrationBuilder.AddColumn<int>(
                name: "ui_port",
                schema: "public",
                table: "device",
                type: "integer",
                nullable: true,
                comment: "Port number of Web UI service");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "cfg",
                type: "varchar(8)",
                nullable: false,
                comment: "Data captured date",
                oldClrType: typeof(string),
                oldType: "varchar(8)");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "cfg",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                comment: "Last updated",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "cfg",
                type: "varchar(6)",
                nullable: false,
                comment: "Data captured time",
                oldClrType: typeof(string),
                oldType: "varchar(6)");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "cfg",
                type: "varchar(20)",
                nullable: false,
                comment: "Device ID",
                oldClrType: typeof(string),
                oldType: "varchar(20)");

            migrationBuilder.AlterColumn<ConfigJson>(
                name: "cfg_file",
                schema: "public",
                table: "cfg",
                type: "jsonb",
                nullable: false,
                comment: "Configuration json file",
                oldClrType: typeof(ConfigJson),
                oldType: "jsonb");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "cfg",
                type: "timestamp",
                nullable: false,
                comment: "Data captured date time",
                oldClrType: typeof(DateTime),
                oldType: "timestamp");

            migrationBuilder.AlterColumn<int>(
                name: "id",
                schema: "public",
                table: "cfg",
                type: "integer",
                nullable: false,
                comment: "Unique ID",
                oldClrType: typeof(int),
                oldType: "integer")
                .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn)
                .OldAnnotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn);

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "box",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                comment: "Last updated",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp");

            migrationBuilder.AlterColumn<float>(
                name: "t_min",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                comment: "Min. temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_y",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                comment: "Y-coordinate value of min. temperature",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_x",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                comment: "X-coordinate value of max. temperature",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<float>(
                name: "t_max",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                comment: "Max. temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_diff",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                comment: "Max. - min temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_avg",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                comment: "Average temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<float>(
                name: "t_90th",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                comment: "90 percentile temperature",
                oldClrType: typeof(float),
                oldType: "real");

            migrationBuilder.AlterColumn<int>(
                name: "p2_y",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                comment: "Y-coordinate value of bottom-right point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "p2_x",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                comment: "X-coordinate value of bottom-right point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "p1_y",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                comment: "Y-coordinate value of top-left point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<int>(
                name: "p1_x",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                comment: "X-coordinate value of top-left point",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "box",
                type: "timestamp",
                nullable: false,
                comment: "Data captured date time",
                oldClrType: typeof(DateTime),
                oldType: "timestamp");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "box",
                type: "varchar(20)",
                nullable: false,
                comment: "Device ID",
                oldClrType: typeof(string),
                oldType: "varchar(20)");

            migrationBuilder.AlterColumn<int>(
                name: "box_id",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                comment: "Box ID",
                oldClrType: typeof(int),
                oldType: "integer");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "box",
                type: "varchar(6)",
                nullable: false,
                comment: "Data captured time",
                oldClrType: typeof(string),
                oldType: "varchar(6)");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "box",
                type: "varchar(8)",
                nullable: false,
                comment: "Data captured date",
                oldClrType: typeof(string),
                oldType: "varchar(8)");

            migrationBuilder.CreateTable(
                name: "box_ctrl",
                schema: "public",
                columns: table => new
                {
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false, comment: "Device ID"),
                    t_max = table.Column<float>(type: "real", nullable: true, comment: "Max. temperature"),
                    insp_r_px = table.Column<int>(type: "integer", nullable: true, comment: "Inspection radius in pixel"),
                    t_warning = table.Column<float>(type: "real", nullable: true, comment: "Warning temp.")
                },
                constraints: table =>
                {
                    table.PrimaryKey("box_ctrl_pkey", x => x.device_id);
                    table.ForeignKey(
                        name: "box_ctrl_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "device_ctrl",
                schema: "public",
                columns: table => new
                {
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false, comment: "Device ID"),
                    lvl_a_to = table.Column<float>(type: "real", nullable: false, comment: "Upper limit of level A"),
                    lvl_b_to = table.Column<float>(type: "real", nullable: false, comment: "Upper limit of level B"),
                    lvl_c_to = table.Column<float>(type: "real", nullable: false, comment: "Upper limit of level C"),
                    montrg_hr = table.Column<int>(type: "integer", nullable: false, comment: "Monitoring hour (up to now)"),
                    nelson_rule = table.Column<string>(type: "varchar(20)", nullable: true, comment: "Nelson rules to apply")
                },
                constraints: table =>
                {
                    table.PrimaryKey("device_ctrl_pkey", x => x.device_id);
                    table.ForeignKey(
                        name: "device_ctrl_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "evnt_log",
                schema: "public",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false, comment: "Unique ID")
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false, comment: "Device ID"),
                    loc_id = table.Column<string>(type: "varchar(10)", nullable: false, comment: "Location ID"),
                    plant_id = table.Column<string>(type: "varchar(10)", nullable: false, comment: "Plant ID"),
                    insp_area = table.Column<string>(type: "varchar(10)", nullable: false, comment: "Inspection Area"),
                    area_id = table.Column<int>(type: "integer", nullable: true, comment: "Area ID"),
                    ymd = table.Column<string>(type: "varchar(8)", nullable: false, comment: "Event date"),
                    hms = table.Column<string>(type: "varchar(6)", nullable: false, comment: "Event time"),
                    evnt_dt = table.Column<DateTime>(type: "timestamp", nullable: false, comment: "Event date time"),
                    evnt_type = table.Column<string>(type: "varchar(15)", nullable: false, comment: "Type of event"),
                    evnt_lvl = table.Column<string>(type: "varchar(15)", nullable: false, comment: "Level of event"),
                    diff_lvl = table.Column<string>(type: "varchar(1)", nullable: false, comment: "Level of diff. temp"),
                    set_value = table.Column<float>(type: "real", nullable: true, comment: "Setting value"),
                    mea_value = table.Column<float>(type: "real", nullable: true, comment: "Measured(actual) value"),
                    title = table.Column<string>(type: "varchar(100)", nullable: false, comment: "Title of event"),
                    msg = table.Column<string>(type: "varchar", nullable: true, comment: "Event message"),
                    emailed_dt = table.Column<DateTime>(type: "timestamp", nullable: true, comment: "Emailed date time"),
                    upd_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp", comment: "Last updated")
                },
                constraints: table =>
                {
                    table.PrimaryKey("evnt_log_pkey", x => x.id);
                    table.ForeignKey(
                        name: "evnt_log_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "frame_ctrl",
                schema: "public",
                columns: table => new
                {
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false, comment: "Device ID"),
                    ucl_i_max = table.Column<float>(type: "real", nullable: true, comment: "UCL of I-chart(max temp. base)"),
                    lcl_i_max = table.Column<float>(type: "real", nullable: true, comment: "LCL of I-chart(max temp. base)"),
                    ucl_mr_max = table.Column<float>(type: "real", nullable: true, comment: "UCL of MR-chart(max temp. base)"),
                    ucl_i_diff = table.Column<float>(type: "real", nullable: true, comment: "UCL of I-chart(diff temp. base)	"),
                    lcl_i_diff = table.Column<float>(type: "real", nullable: true, comment: "LCL of I-chart(diff temp. base)"),
                    ucl_mr_diff = table.Column<float>(type: "real", nullable: true, comment: "UCL of MR-chart(diff temp. base)"),
                    t_warning = table.Column<float>(type: "real", nullable: true, comment: "Warning temp.")
                },
                constraints: table =>
                {
                    table.PrimaryKey("frame_ctrl_pkey", x => x.device_id);
                    table.ForeignKey(
                        name: "frame_ctrl_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "grp_key",
                schema: "public",
                columns: table => new
                {
                    grp = table.Column<string>(type: "varchar(30)", nullable: false, comment: "Group name"),
                    key = table.Column<string>(type: "varchar(50)", nullable: false, comment: "Key value"),
                    desn = table.Column<string>(type: "varchar(100)", nullable: true, comment: "Description"),
                    ord = table.Column<int>(type: "integer", nullable: true, comment: "Order by"),
                    inactive = table.Column<bool>(type: "boolean", nullable: false, comment: "Inactive status")
                },
                constraints: table =>
                {
                    table.PrimaryKey("grp_key_pkey", x => new { x.grp, x.key });
                });

            migrationBuilder.CreateTable(
                name: "mailing_addr",
                schema: "public",
                columns: table => new
                {
                    plant_id = table.Column<string>(type: "varchar(10)", nullable: false, comment: "Plant ID"),
                    email = table.Column<string>(type: "varchar(100)", nullable: false, comment: "Email"),
                    name = table.Column<string>(type: "varchar(30)", nullable: false, comment: "Full name"),
                    tel = table.Column<string>(type: "varchar(30)", nullable: true, comment: "Telephone number"),
                    team = table.Column<string>(type: "varchar(30)", nullable: true, comment: "Team name"),
                    inactive = table.Column<bool>(type: "boolean", nullable: false, comment: "Inactive status")
                },
                constraints: table =>
                {
                    table.PrimaryKey("mailing_addr_pkey", x => new { x.plant_id, x.email });
                });

            migrationBuilder.CreateTable(
                name: "roi_ctrl",
                schema: "public",
                columns: table => new
                {
                    device_id = table.Column<string>(type: "varchar(20)", nullable: false, comment: "Device ID"),
                    roi_id = table.Column<int>(type: "integer", nullable: false, comment: "ROI ID"),
                    ucl_i_max = table.Column<float>(type: "real", nullable: true, comment: "UCL of I-chart(max temp. base)"),
                    lcl_i_max = table.Column<float>(type: "real", nullable: true, comment: "LCL of I-chart(max temp. base)"),
                    ucl_mr_max = table.Column<float>(type: "real", nullable: true, comment: "UCL of MR-chart(max temp. base)"),
                    ucl_i_diff = table.Column<float>(type: "real", nullable: true, comment: "UCL of I-chart(diff temp. base)"),
                    lcl_i_diff = table.Column<float>(type: "real", nullable: true, comment: "LCL of I-chart(diff temp. base)"),
                    ucl_mr_diff = table.Column<float>(type: "real", nullable: true, comment: "UCL of MR-chart(diff temp. base)"),
                    t_warning = table.Column<float>(type: "real", nullable: true, comment: "Warning temp.")
                },
                constraints: table =>
                {
                    table.PrimaryKey("roi_ctrl_pkey", x => new { x.device_id, x.roi_id });
                    table.ForeignKey(
                        name: "roi_ctrl_device_id_fkey",
                        column: x => x.device_id,
                        principalSchema: "public",
                        principalTable: "device",
                        principalColumn: "device_id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.InsertData(
                schema: "public",
                table: "grp_key",
                columns: new[] { "grp", "key", "desn", "inactive", "ord" },
                values: new object[,]
                {
                    { "plant_id", "HQ", null, false, null },
                    { "event_lvl", "Warning", null, false, null },
                    { "event_lvl", "Urgent", null, false, null },
                    { "event_type", "NelsonRule", "Nelson rule", false, null },
                    { "event_type", "LCL", "Lower control limit", false, null },
                    { "event_type", "UCL", "Upper control limit", false, null },
                    { "event_type", "DiffLevel", "Diff. level", false, null },
                    { "event_type", "MaxTemp", "Max. temperature", false, null },
                    { "file_type", "jpg", null, false, null },
                    { "file_type", "csv", null, false, null },
                    { "event_lvl", "Info", null, false, null },
                    { "file_type", "json", null, false, null },
                    { "medium_type", "temp", "Temperature summary json", false, null },
                    { "medium_type", "cfg", "Confiruation file", false, null },
                    { "medium_type", "rgb", "RGB camera image", false, null },
                    { "medium_type", "ir", "IR camera image", false, null },
                    { "plant_id", "QD", null, false, null },
                    { "plant_id", "RJ", null, false, null },
                    { "plant_id", "JJS", null, false, null },
                    { "plant_id", "CKP", null, false, null },
                    { "plant_id", "JJ", null, false, null },
                    { "plant_id", "VJ", null, false, null },
                    { "medium_type", "raw", "Temperature raw csv", false, null },
                    { "event_lvl", "Error", null, false, null }
                });

            migrationBuilder.CreateIndex(
                name: "box_ctrl_device_id_idx",
                schema: "public",
                table: "box_ctrl",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "device_ctrl_device_id_idx",
                schema: "public",
                table: "device_ctrl",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "evnt_log_device_id_idx",
                schema: "public",
                table: "evnt_log",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "evnt_log_ymd_hms_idx",
                schema: "public",
                table: "evnt_log",
                columns: new[] { "ymd", "hms" });

            migrationBuilder.CreateIndex(
                name: "ix_ymd_hms1",
                schema: "public",
                table: "evnt_log",
                columns: new[] { "ymd", "hms" });

            migrationBuilder.CreateIndex(
                name: "frame_ctrl_device_id_idx",
                schema: "public",
                table: "frame_ctrl",
                column: "device_id");

            migrationBuilder.CreateIndex(
                name: "roi_ctrl_device_id_idx",
                schema: "public",
                table: "roi_ctrl",
                column: "device_id");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "box_ctrl",
                schema: "public");

            migrationBuilder.DropTable(
                name: "device_ctrl",
                schema: "public");

            migrationBuilder.DropTable(
                name: "evnt_log",
                schema: "public");

            migrationBuilder.DropTable(
                name: "frame_ctrl",
                schema: "public");

            migrationBuilder.DropTable(
                name: "grp_key",
                schema: "public");

            migrationBuilder.DropTable(
                name: "mailing_addr",
                schema: "public");

            migrationBuilder.DropTable(
                name: "roi_ctrl",
                schema: "public");

            migrationBuilder.DropColumn(
                name: "t_min_x",
                schema: "public",
                table: "frame");

            migrationBuilder.DropColumn(
                name: "t_min_y",
                schema: "public",
                table: "frame");

            migrationBuilder.DropColumn(
                name: "api_port",
                schema: "public",
                table: "device");

            migrationBuilder.DropColumn(
                name: "ip_addr",
                schema: "public",
                table: "device");

            migrationBuilder.DropColumn(
                name: "ui_port",
                schema: "public",
                table: "device");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "roi",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp",
                oldComment: "Last updated");

            migrationBuilder.AlterColumn<float>(
                name: "t_min",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Min. temperature");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_y",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Y-coordinate value of min. temperature");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_x",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "X-coordinate value of max. temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_max",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Max. temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_diff",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Max. - min temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_avg",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Average temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_90th",
                schema: "public",
                table: "roi",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "90 percentile temperature");

            migrationBuilder.AlterColumn<int>(
                name: "p2_y",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Y-coordinate value of bottom-right point");

            migrationBuilder.AlterColumn<int>(
                name: "p2_x",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "X-coordinate value of bottom-right point");

            migrationBuilder.AlterColumn<int>(
                name: "p1_y",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Y-coordinate value of top-left point");

            migrationBuilder.AlterColumn<int>(
                name: "p1_x",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "X-coordinate value of top-left point");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "roi",
                type: "timestamp",
                nullable: false,
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldComment: "Data captured date time");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "roi",
                type: "varchar(20)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(20)",
                oldComment: "Device ID");

            migrationBuilder.AlterColumn<int>(
                name: "roi_id",
                schema: "public",
                table: "roi",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "ROI ID");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "roi",
                type: "varchar(6)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(6)",
                oldComment: "Data captured time");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "roi",
                type: "varchar(8)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(8)",
                oldComment: "Data captured date");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "medium",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp",
                oldComment: "Last updated");

            migrationBuilder.AlterColumn<string>(
                name: "file_type",
                schema: "public",
                table: "medium",
                type: "varchar(20)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(20)",
                oldComment: "File type");

            migrationBuilder.AlterColumn<long>(
                name: "file_size",
                schema: "public",
                table: "medium",
                type: "bigint",
                nullable: false,
                oldClrType: typeof(long),
                oldType: "bigint",
                oldComment: "File size in bytes");

            migrationBuilder.AlterColumn<string>(
                name: "file_name",
                schema: "public",
                table: "medium",
                type: "varchar(255)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(255)",
                oldComment: "File name");

            migrationBuilder.AlterColumn<byte[]>(
                name: "file_content",
                schema: "public",
                table: "medium",
                type: "bytea",
                nullable: false,
                oldClrType: typeof(byte[]),
                oldType: "bytea",
                oldComment: "File content");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "medium",
                type: "timestamp",
                nullable: false,
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldComment: "Data captured date time");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "medium",
                type: "varchar(20)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(20)",
                oldComment: "Device ID");

            migrationBuilder.AlterColumn<string>(
                name: "medium_type",
                schema: "public",
                table: "medium",
                type: "varchar(10)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(10)",
                oldComment: "Medium type");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "medium",
                type: "varchar(6)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(6)",
                oldComment: "Data captured time");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "medium",
                type: "varchar(8)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(8)",
                oldComment: "Data captured date");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "frame",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp",
                oldComment: "Last updated");

            migrationBuilder.AlterColumn<float>(
                name: "t_min",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Min. temperature");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_y",
                schema: "public",
                table: "frame",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Y-coordinate value of min. temperature");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_x",
                schema: "public",
                table: "frame",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "X-coordinate value of max. temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_max",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Max. temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_diff",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Max. - min temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_avg",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Average temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_90th",
                schema: "public",
                table: "frame",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "90 percentile temperature");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "frame",
                type: "timestamp",
                nullable: false,
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldComment: "Data captured date time");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "frame",
                type: "varchar(20)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(20)",
                oldComment: "Device ID");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "frame",
                type: "varchar(6)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(6)",
                oldComment: "Data captured time");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "frame",
                type: "varchar(8)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(8)",
                oldComment: "Data captured date");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "device",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp",
                oldComment: "Last updated");

            migrationBuilder.AlterColumn<string>(
                name: "root_path",
                schema: "public",
                table: "device",
                type: "varchar(255)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(255)",
                oldComment: "Root path of data directory");

            migrationBuilder.AlterColumn<string>(
                name: "plant_id",
                schema: "public",
                table: "device",
                type: "varchar(10)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(10)",
                oldComment: "Plant ID");

            migrationBuilder.AlterColumn<int>(
                name: "ord",
                schema: "public",
                table: "device",
                type: "integer",
                nullable: true,
                oldClrType: typeof(int),
                oldType: "integer",
                oldNullable: true,
                oldComment: "Order by");

            migrationBuilder.AlterColumn<string>(
                name: "name",
                schema: "public",
                table: "device",
                type: "varchar(30)",
                nullable: true,
                oldClrType: typeof(string),
                oldType: "varchar(30)",
                oldNullable: true,
                oldComment: "Name of device");

            migrationBuilder.AlterColumn<string>(
                name: "loc_id",
                schema: "public",
                table: "device",
                type: "varchar(10)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(10)",
                oldComment: "Location ID");

            migrationBuilder.AlterColumn<string>(
                name: "desn",
                schema: "public",
                table: "device",
                type: "varchar(200)",
                nullable: true,
                oldClrType: typeof(string),
                oldType: "varchar(200)",
                oldNullable: true,
                oldComment: "Description");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "device",
                type: "varchar(20)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(20)",
                oldComment: "Device ID");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "cfg",
                type: "varchar(8)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(8)",
                oldComment: "Data captured date");

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "cfg",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp",
                oldComment: "Last updated");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "cfg",
                type: "varchar(6)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(6)",
                oldComment: "Data captured time");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "cfg",
                type: "varchar(20)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(20)",
                oldComment: "Device ID");

            migrationBuilder.AlterColumn<ConfigJson>(
                name: "cfg_file",
                schema: "public",
                table: "cfg",
                type: "jsonb",
                nullable: false,
                oldClrType: typeof(ConfigJson),
                oldType: "jsonb",
                oldComment: "Configuration json file");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "cfg",
                type: "timestamp",
                nullable: false,
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldComment: "Data captured date time");

            migrationBuilder.AlterColumn<int>(
                name: "id",
                schema: "public",
                table: "cfg",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Unique ID")
                .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn)
                .OldAnnotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn);

            migrationBuilder.AlterColumn<DateTime>(
                name: "upd_dt",
                schema: "public",
                table: "box",
                type: "timestamp",
                nullable: false,
                defaultValueSql: "current_timestamp",
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldDefaultValueSql: "current_timestamp",
                oldComment: "Last updated");

            migrationBuilder.AlterColumn<float>(
                name: "t_min",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Min. temperature");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_y",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Y-coordinate value of min. temperature");

            migrationBuilder.AlterColumn<int>(
                name: "t_max_x",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "X-coordinate value of max. temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_max",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Max. temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_diff",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Max. - min temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_avg",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "Average temperature");

            migrationBuilder.AlterColumn<float>(
                name: "t_90th",
                schema: "public",
                table: "box",
                type: "real",
                nullable: false,
                oldClrType: typeof(float),
                oldType: "real",
                oldComment: "90 percentile temperature");

            migrationBuilder.AlterColumn<int>(
                name: "p2_y",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Y-coordinate value of bottom-right point");

            migrationBuilder.AlterColumn<int>(
                name: "p2_x",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "X-coordinate value of bottom-right point");

            migrationBuilder.AlterColumn<int>(
                name: "p1_y",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Y-coordinate value of top-left point");

            migrationBuilder.AlterColumn<int>(
                name: "p1_x",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "X-coordinate value of top-left point");

            migrationBuilder.AlterColumn<DateTime>(
                name: "capture_dt",
                schema: "public",
                table: "box",
                type: "timestamp",
                nullable: false,
                oldClrType: typeof(DateTime),
                oldType: "timestamp",
                oldComment: "Data captured date time");

            migrationBuilder.AlterColumn<string>(
                name: "device_id",
                schema: "public",
                table: "box",
                type: "varchar(20)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(20)",
                oldComment: "Device ID");

            migrationBuilder.AlterColumn<int>(
                name: "box_id",
                schema: "public",
                table: "box",
                type: "integer",
                nullable: false,
                oldClrType: typeof(int),
                oldType: "integer",
                oldComment: "Box ID");

            migrationBuilder.AlterColumn<string>(
                name: "hms",
                schema: "public",
                table: "box",
                type: "varchar(6)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(6)",
                oldComment: "Data captured time");

            migrationBuilder.AlterColumn<string>(
                name: "ymd",
                schema: "public",
                table: "box",
                type: "varchar(8)",
                nullable: false,
                oldClrType: typeof(string),
                oldType: "varchar(8)",
                oldComment: "Data captured date");
        }
    }
}
