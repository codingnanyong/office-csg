namespace CSG.MI.TrMontrgSrv.Model.Dashboard
{
    public class CurDevice : BaseModel
    {
        public string DeviceId { get; set; }

        public string LocationId { get; set; }

        public string PlantId { get; set; }

        public string Description { get; set; }

        public CurFrame Frame { get; set; }

        /* Not-Use
        public ICollection<CurBox> Boxes { get; set; }

        public ICollection<CurRoi> Roies { get; set; }
        */
    }
}
