using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EF.Migrations
{
    public partial class added_index : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateIndex(
                name: "roi_device_id_roi_id_capture_dt_idx",
                schema: "public",
                table: "roi",
                columns: new[] { "device_id", "roi_id", "capture_dt" });

            migrationBuilder.CreateIndex(
                name: "frame_device_id_capture_dt_idx",
                schema: "public",
                table: "frame",
                columns: new[] { "device_id", "capture_dt" });

            migrationBuilder.CreateIndex(
                name: "evnt_log_evnt_dt_idx",
                schema: "public",
                table: "evnt_log",
                column: "evnt_dt");

            migrationBuilder.CreateIndex(
                name: "evnt_lot_device_id_evnt_dt_evnt_lvl_idx",
                schema: "public",
                table: "evnt_log",
                columns: new[] { "device_id", "evnt_dt", "evnt_lvl" });

            migrationBuilder.CreateIndex(
                name: "evnt_lot_device_id_evnt_dt_idx",
                schema: "public",
                table: "evnt_log",
                columns: new[] { "device_id", "evnt_dt" });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "roi_device_id_roi_id_capture_dt_idx",
                schema: "public",
                table: "roi");

            migrationBuilder.DropIndex(
                name: "frame_device_id_capture_dt_idx",
                schema: "public",
                table: "frame");

            migrationBuilder.DropIndex(
                name: "evnt_log_evnt_dt_idx",
                schema: "public",
                table: "evnt_log");

            migrationBuilder.DropIndex(
                name: "evnt_lot_device_id_evnt_dt_evnt_lvl_idx",
                schema: "public",
                table: "evnt_log");

            migrationBuilder.DropIndex(
                name: "evnt_lot_device_id_evnt_dt_idx",
                schema: "public",
                table: "evnt_log");
        }
    }
}
