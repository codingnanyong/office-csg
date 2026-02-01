using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CSG.MI.TrMontrgSrv.EF.Migrations
{
    public partial class SProcedure : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            var assembly = Assembly.GetExecutingAssembly();
            var files = assembly.GetManifestResourceNames().Where(f => f.EndsWith(".sql"));
            foreach (var file in files)
            {
                using Stream stream = assembly.GetManifestResourceStream(file);
                using StreamReader reader = new(stream);
                var sql = reader.ReadToEnd();
                migrationBuilder.Sql($"{sql}");
                Console.WriteLine($"Created {file}");
            }
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("DROP FUNCTION IF EXISTS fn_frame_imr_data;");
            migrationBuilder.Sql("DROP FUNCTION IF EXISTS fn_roi_imr_data;");
        }
    }
}
