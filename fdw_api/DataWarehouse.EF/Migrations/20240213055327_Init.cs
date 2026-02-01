using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace CSG.MI.FDW.EF.Migrations
{
    /// <inheritdoc />
    public partial class Init : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "feedback");

            migrationBuilder.CreateTable(
                name: "category_mst",
                schema: "feedback",
                columns: table => new
                {
                    key = table.Column<string>(type: "varchar(5)", nullable: false),
                    value_kor = table.Column<string>(type: "varchar(30)", nullable: false),
                    value_eng = table.Column<string>(type: "varchar(30)", nullable: false),
                    edit_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_category", x => x.key);
                });

            migrationBuilder.CreateTable(
                name: "system",
                schema: "feedback",
                columns: table => new
                {
                    id = table.Column<int>(type: "integer", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    name = table.Column<string>(type: "varchar(20)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_system", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "user_feedback",
                schema: "feedback",
                columns: table => new
                {
                    seq = table.Column<long>(type: "BigInt", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    system = table.Column<int>(type: "integer", nullable: false),
                    category = table.Column<string>(type: "varchar(30)", nullable: false),
                    comment = table.Column<string>(type: "text", nullable: true),
                    edit_dt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "current_timestamp")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_feedback", x => x.seq);
                    table.ForeignKey(
                        name: "FK_feedback_category",
                        column: x => x.category,
                        principalSchema: "feedback",
                        principalTable: "category_mst",
                        principalColumn: "key",
                        onDelete: ReferentialAction.SetNull);
                    table.ForeignKey(
                        name: "FK_feedback_system",
                        column: x => x.system,
                        principalSchema: "feedback",
                        principalTable: "system",
                        principalColumn: "id",
                        onDelete: ReferentialAction.SetNull);
                });

            migrationBuilder.CreateIndex(
                name: "IX_feedback_edit_date",
                schema: "feedback",
                table: "user_feedback",
                column: "edit_dt");

            migrationBuilder.CreateIndex(
                name: "IX_user_feedback_category",
                schema: "feedback",
                table: "user_feedback",
                column: "category");

            migrationBuilder.CreateIndex(
                name: "IX_user_feedback_system",
                schema: "feedback",
                table: "user_feedback",
                column: "system");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "user_feedback",
                schema: "feedback");

            migrationBuilder.DropTable(
                name: "category_mst",
                schema: "feedback");

            migrationBuilder.DropTable(
                name: "system",
                schema: "feedback");
        }
    }
}
