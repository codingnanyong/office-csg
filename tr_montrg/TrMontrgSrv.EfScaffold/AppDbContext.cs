using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using CSG.MI.TrMontrgSrv.EfScaffold.Models;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold
{
    public partial class AppDbContext : DbContext
    {
        public AppDbContext()
        {
        }

        public AppDbContext(DbContextOptions<AppDbContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Box> Boxes { get; set; }
        public virtual DbSet<BoxCtrl> BoxCtrls { get; set; }
        public virtual DbSet<Cfg> Cfgs { get; set; }
        public virtual DbSet<Device> Devices { get; set; }
        public virtual DbSet<DeviceCtrl> DeviceCtrls { get; set; }
        public virtual DbSet<EvntLog> EvntLogs { get; set; }
        public virtual DbSet<Frame> Frames { get; set; }
        public virtual DbSet<FrameCtrl> FrameCtrls { get; set; }
        public virtual DbSet<GrpKey> GrpKeys { get; set; }
        public virtual DbSet<MailingAddr> MailingAddrs { get; set; }
        public virtual DbSet<Medium> Media { get; set; }
        public virtual DbSet<Roi> Rois { get; set; }
        public virtual DbSet<RoiCtrl> RoiCtrls { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                // Use appsettings.json or environment variables for connection string
                // Example: optionsBuilder.UseNpgsql("Host={DB_HOST};Port={DB_PORT};Database=tr_montrg_srv;Username={DB_USER};Password={DB_PASSWORD}");
                optionsBuilder.UseNpgsql("Host=localhost;Port=5433;Database=tr_montrg_srv;Username={DB_USER};Password={DB_PASSWORD}");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.HasAnnotation("Relational:Collation", "en_US.utf8");

            modelBuilder.Entity<Box>(entity =>
            {
                entity.HasKey(e => new { e.Ymd, e.Hms, e.BoxId, e.DeviceId })
                    .HasName("box_pkey");

                entity.Property(e => e.Ymd).HasComment("Data captured date");

                entity.Property(e => e.Hms).HasComment("Data captured time");

                entity.Property(e => e.BoxId).HasComment("Box ID");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.CaptureDt).HasComment("Data captured date time");

                entity.Property(e => e.P1X).HasComment("X-coordinate value of top-left point");

                entity.Property(e => e.P1Y).HasComment("Y-coordinate value of top-left point");

                entity.Property(e => e.P2X).HasComment("X-coordinate value of bottom-right point");

                entity.Property(e => e.P2Y).HasComment("Y-coordinate value of bottom-right point");

                entity.Property(e => e.T90th).HasComment("90 percentile temperature");

                entity.Property(e => e.TAvg).HasComment("Average temperature");

                entity.Property(e => e.TDiff).HasComment("Max. - min temperature");

                entity.Property(e => e.TMax).HasComment("Max. temperature");

                entity.Property(e => e.TMaxX).HasComment("X-coordinate value of max. temperature");

                entity.Property(e => e.TMaxY).HasComment("Y-coordinate value of min. temperature");

                entity.Property(e => e.TMin).HasComment("Min. temperature");

                entity.Property(e => e.UpdDt)
                    .HasDefaultValueSql("CURRENT_TIMESTAMP")
                    .HasComment("Last updated");

                entity.HasOne(d => d.Device)
                    .WithMany(p => p.Boxes)
                    .HasForeignKey(d => d.DeviceId)
                    .HasConstraintName("box_device_id_fkey");
            });

            modelBuilder.Entity<BoxCtrl>(entity =>
            {
                entity.HasKey(e => e.DeviceId)
                    .HasName("box_ctrl_pkey");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.InspRPx).HasComment("Inspection radius in pixel");

                entity.Property(e => e.TMax).HasComment("Max. temperature");

                entity.HasOne(d => d.Device)
                    .WithOne(p => p.BoxCtrl)
                    .HasForeignKey<BoxCtrl>(d => d.DeviceId)
                    .HasConstraintName("box_ctrl_device_id_fkey");
            });

            modelBuilder.Entity<Cfg>(entity =>
            {
                entity.Property(e => e.Id).HasComment("Unique ID");

                entity.Property(e => e.CaptureDt).HasComment("Data captured date time");

                entity.Property(e => e.CfgFile).HasComment("Configuration json file");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.Hms).HasComment("Data captured time");

                entity.Property(e => e.UpdDt)
                    .HasDefaultValueSql("CURRENT_TIMESTAMP")
                    .HasComment("Last updated");

                entity.Property(e => e.Ymd).HasComment("Data captured date");

                entity.HasOne(d => d.Device)
                    .WithMany(p => p.Cfgs)
                    .HasForeignKey(d => d.DeviceId)
                    .HasConstraintName("cfg_device_id_fkey");
            });

            modelBuilder.Entity<Device>(entity =>
            {
                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.ApiPort).HasComment("Port number of RESTful API service");

                entity.Property(e => e.Desn).HasComment("Description");

                entity.Property(e => e.IpAddr).HasComment("IP address");

                entity.Property(e => e.LocId).HasComment("Location ID");

                entity.Property(e => e.Name).HasComment("Name of device");

                entity.Property(e => e.Ord).HasComment("Order by");

                entity.Property(e => e.PlantId).HasComment("Plant ID");

                entity.Property(e => e.RootPath).HasComment("Root path of data directory");

                entity.Property(e => e.UiPort).HasComment("Port number of Web UI service");

                entity.Property(e => e.UpdDt)
                    .HasDefaultValueSql("CURRENT_TIMESTAMP")
                    .HasComment("Last updated");
            });

            modelBuilder.Entity<DeviceCtrl>(entity =>
            {
                entity.HasKey(e => e.DeviceId)
                    .HasName("device_ctrl_pkey");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.LvlATo).HasComment("Upper limit of level A");

                entity.Property(e => e.LvlBTo).HasComment("Upper limit of level B");

                entity.Property(e => e.LvlCTo).HasComment("Upper limit of level C");

                entity.Property(e => e.MontrgHr).HasComment("Monitoring hour (up to now)");

                entity.Property(e => e.NelsonRule).HasComment("Nelson rules to apply");

                entity.HasOne(d => d.Device)
                    .WithOne(p => p.DeviceCtrl)
                    .HasForeignKey<DeviceCtrl>(d => d.DeviceId)
                    .HasConstraintName("device_ctrl_device_id_fkey");
            });

            modelBuilder.Entity<EvntLog>(entity =>
            {
                entity.Property(e => e.Id).HasComment("Unique ID");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.EmailedDt).HasComment("Emailed date time");

                entity.Property(e => e.EvntDt).HasComment("Event date time");

                entity.Property(e => e.EvntLvl).HasComment("Level of event");

                entity.Property(e => e.EvntType).HasComment("Type of event");

                entity.Property(e => e.Hms).HasComment("Event time");

                entity.Property(e => e.MeaValue).HasComment("Measured(actual) value");

                entity.Property(e => e.Msg).HasComment("Event message");

                entity.Property(e => e.SetValue).HasComment("Setting value");

                entity.Property(e => e.Title).HasComment("Title of event");

                entity.Property(e => e.Ymd).HasComment("Event date");

                entity.HasOne(d => d.Device)
                    .WithMany(p => p.EvntLogs)
                    .HasForeignKey(d => d.DeviceId)
                    .HasConstraintName("evnt_log_device_id_fkey");
            });

            modelBuilder.Entity<Frame>(entity =>
            {
                entity.HasKey(e => new { e.Ymd, e.Hms, e.DeviceId })
                    .HasName("frame_pkey");

                entity.Property(e => e.Ymd).HasComment("Data captured date");

                entity.Property(e => e.Hms).HasComment("Data captured time");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.CaptureDt).HasComment("Data captured date time");

                entity.Property(e => e.T90th).HasComment("90 percentile temperature");

                entity.Property(e => e.TAvg).HasComment("Average temperature");

                entity.Property(e => e.TDiff).HasComment("Max. - min temperature");

                entity.Property(e => e.TMax).HasComment("Max. temperature");

                entity.Property(e => e.TMaxX).HasComment("X-coordinate value of max. temperature");

                entity.Property(e => e.TMaxY).HasComment("Y-coordinate value of min. temperature");

                entity.Property(e => e.TMin).HasComment("Min. temperature");

                entity.Property(e => e.TMinX).HasComment("X-coordinate value of min. temperature");

                entity.Property(e => e.TMinY).HasComment("Y-coordinate value of min. temperature");

                entity.Property(e => e.UpdDt)
                    .HasDefaultValueSql("CURRENT_TIMESTAMP")
                    .HasComment("Last updated");

                entity.HasOne(d => d.Device)
                    .WithMany(p => p.Frames)
                    .HasForeignKey(d => d.DeviceId)
                    .HasConstraintName("frame_device_id_fkey");
            });

            modelBuilder.Entity<FrameCtrl>(entity =>
            {
                entity.HasKey(e => e.DeviceId)
                    .HasName("frame_ctrl_pkey");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.LclIDiff).HasComment("LCL of I-chart(diff temp. base)");

                entity.Property(e => e.LclIMax).HasComment("LCL of I-chart(max temp. base)");

                entity.Property(e => e.UclIDiff).HasComment("UCL of I-chart(diff temp. base)	");

                entity.Property(e => e.UclIMax).HasComment("UCL of I-chart(max temp. base)");

                entity.Property(e => e.UclMrDiff).HasComment("UCL of MR-chart(diff temp. base)");

                entity.Property(e => e.UclMrMax).HasComment("UCL of MR-chart(max temp. base)");

                entity.HasOne(d => d.Device)
                    .WithOne(p => p.FrameCtrl)
                    .HasForeignKey<FrameCtrl>(d => d.DeviceId)
                    .HasConstraintName("frame_ctrl_device_id_fkey");
            });

            modelBuilder.Entity<GrpKey>(entity =>
            {
                entity.HasKey(e => new { e.Grp, e.Key })
                    .HasName("grp_key_pkey");

                entity.Property(e => e.Grp).HasComment("Group name");

                entity.Property(e => e.Key).HasComment("Key value");

                entity.Property(e => e.Desn).HasComment("Description");

                entity.Property(e => e.Inactive).HasComment("Inactive status");

                entity.Property(e => e.Ord).HasComment("Order by");
            });

            modelBuilder.Entity<MailingAddr>(entity =>
            {
                entity.HasKey(e => new { e.PlantId, e.Email })
                    .HasName("mailing_addr_pkey");

                entity.Property(e => e.PlantId).HasComment("Plant ID");

                entity.Property(e => e.Email).HasComment("Email");

                entity.Property(e => e.Inactive).HasComment("Inactive status");

                entity.Property(e => e.Name).HasComment("Full name");

                entity.Property(e => e.Team).HasComment("Team name");

                entity.Property(e => e.Tel).HasComment("Telephone number");
            });

            modelBuilder.Entity<Medium>(entity =>
            {
                entity.HasKey(e => new { e.Ymd, e.Hms, e.MediumType, e.DeviceId })
                    .HasName("medium_pkey");

                entity.Property(e => e.Ymd).HasComment("Data captured date");

                entity.Property(e => e.Hms).HasComment("Data captured time");

                entity.Property(e => e.MediumType).HasComment("Medium type");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.CaptureDt).HasComment("Data captured date time");

                entity.Property(e => e.FileContent).HasComment("File content");

                entity.Property(e => e.FileName).HasComment("File name");

                entity.Property(e => e.FileSize).HasComment("File size in bytes");

                entity.Property(e => e.FileType).HasComment("File type");

                entity.Property(e => e.UpdDt)
                    .HasDefaultValueSql("CURRENT_TIMESTAMP")
                    .HasComment("Last updated");

                entity.HasOne(d => d.Device)
                    .WithMany(p => p.Media)
                    .HasForeignKey(d => d.DeviceId)
                    .HasConstraintName("medium_device_id_fkey");
            });

            modelBuilder.Entity<Roi>(entity =>
            {
                entity.HasKey(e => new { e.Ymd, e.Hms, e.RoiId, e.DeviceId })
                    .HasName("roi_pkey");

                entity.Property(e => e.Ymd).HasComment("Data captured date");

                entity.Property(e => e.Hms).HasComment("Data captured time");

                entity.Property(e => e.RoiId).HasComment("ROI ID");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.CaptureDt).HasComment("Data captured date time");

                entity.Property(e => e.P1X).HasComment("X-coordinate value of top-left point");

                entity.Property(e => e.P1Y).HasComment("Y-coordinate value of top-left point");

                entity.Property(e => e.P2X).HasComment("X-coordinate value of bottom-right point");

                entity.Property(e => e.P2Y).HasComment("Y-coordinate value of bottom-right point");

                entity.Property(e => e.T90th).HasComment("90 percentile temperature");

                entity.Property(e => e.TAvg).HasComment("Average temperature");

                entity.Property(e => e.TDiff).HasComment("Max. - min temperature");

                entity.Property(e => e.TMax).HasComment("Max. temperature");

                entity.Property(e => e.TMaxX).HasComment("X-coordinate value of max. temperature");

                entity.Property(e => e.TMaxY).HasComment("Y-coordinate value of min. temperature");

                entity.Property(e => e.TMin).HasComment("Min. temperature");

                entity.Property(e => e.UpdDt)
                    .HasDefaultValueSql("CURRENT_TIMESTAMP")
                    .HasComment("Last updated");

                entity.HasOne(d => d.Device)
                    .WithMany(p => p.Rois)
                    .HasForeignKey(d => d.DeviceId)
                    .HasConstraintName("roi_device_id_fkey");
            });

            modelBuilder.Entity<RoiCtrl>(entity =>
            {
                entity.HasKey(e => new { e.DeviceId, e.RoiId })
                    .HasName("roi_ctrl_pkey");

                entity.Property(e => e.DeviceId).HasComment("Device ID");

                entity.Property(e => e.RoiId).HasComment("ROI ID");

                entity.Property(e => e.LclIDiff).HasComment("LCL of I-chart(diff temp. base)");

                entity.Property(e => e.LclIMax).HasComment("LCL of I-chart(max temp. base)");

                entity.Property(e => e.UclIDiff).HasComment("UCL of I-chart(diff temp. base)");

                entity.Property(e => e.UclIMax).HasComment("UCL of I-chart(max temp. base)");

                entity.Property(e => e.UclMrDiff).HasComment("UCL of MR-chart(diff temp. base)");

                entity.Property(e => e.UclMrMax).HasComment("UCL of MR-chart(max temp. base)");

                entity.HasOne(d => d.Device)
                    .WithMany(p => p.RoiCtrls)
                    .HasForeignKey(d => d.DeviceId)
                    .HasConstraintName("roi_ctrl_device_id_fkey");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
