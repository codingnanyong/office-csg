namespace CSG.MI.TrMontrgSrv.Model.Dashboard
{
    public abstract class BaseTemp : BaseModel
    {
        public string Id { get; set; }

        public float? Avg { get; set; }

        public float? Max { get; set; }

        public float? Min { get; set; }

        public float? Dif { get; set; }

         public float? d_Avg { get; set; }

         public float? d_Max { get; set; }

         public float? d_Min { get; set; }

         public float? d_Diff { get; set; }
    }
}
