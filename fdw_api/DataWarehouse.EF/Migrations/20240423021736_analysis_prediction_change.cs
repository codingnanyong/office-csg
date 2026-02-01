using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSG.MI.FDW.EF.Migrations
{
    /// <inheritdoc />
    public partial class analysis_prediction_change : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameTable(
                name: "dev_style_prediction",
                schema: "services",
                newName: "prediction_devstyle",
                newSchema: "services");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameTable(
                name: "prediction_devstyle",
                schema: "services",
                newName: "dev_style_prediction",
                newSchema: "services");
        }
    }
}
