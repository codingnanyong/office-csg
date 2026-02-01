namespace CSG.MI.DAO.Production.RTLS
{
    public class ErpPlan : BaseModel
    {
        public long Id { get; set; } = 0;

        public DateTime CreateTime { get; set; } = DateTime.Now;

        public string Factory { get; set; } = "DS";

        public string Wsno { get; set; } = String.Empty;

        public string Passtype { get; set; } = "B";

        public decimal Seq { get; set; } = Decimal.Zero;

        public string Season { get; set; } = String.Empty;

        public string Modelname { get; set; } = String.Empty;

        public string ColorwayId { get; set; } = String.Empty;

        public string Stcd { get; set; } = String.Empty;

        public string Substcd { get; set; } = String.Empty;

        public string BoxNo { get; set; } = "0";

        public string Pm { get; set; } = String.Empty;

        public string Pe { get; set; } = String.Empty;

        public string SizeCd { get; set; } = String.Empty;

        public string Gender { get; set; } = String.Empty;

        public decimal PcardQty { get; set; } = Decimal.Zero;

        public string SampleEts { get; set; } = String.Empty;

        public string SubRemark { get; set; } = String.Empty;

        public string ProdFactory { get; set; } = String.Empty;

        public string Category { get; set; } = String.Empty;

        public string PlanDate { get; set; } = String.Empty;

        public string BeaconTagId { get; set; } = String.Empty;

        public string Te { get; set; } = String.Empty;

        public string Ce { get; set; } = String.Empty;
    }
}
