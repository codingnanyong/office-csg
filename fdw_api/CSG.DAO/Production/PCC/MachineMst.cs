namespace CSG.MI.DAO.Production.PCC
{
    public class MachineMst : BaseModel
    {
        public decimal Seq { get; set; } = Decimal.Zero;

        public string Dept { get; set; } = String.Empty;

        public string Place { get; set; } = String.Empty;

        public string Name { get; set; } = String.Empty;

        public string NameEng { get; set; } = String.Empty;

        public string Division { get; set; } = String.Empty;

        public string Main { get; set; } = String.Empty;

        public string Sub { get; set; } = String.Empty;

        public string Weight { get; set; } = String.Empty;

        public string Capacitance { get; set; } = String.Empty;

        public string Type { get; set; } = String.Empty;

        public string Dimension { get; set; } = String.Empty;

        public string Power { get; set; } = String.Empty;

        public string AssetNo { get; set; } = String.Empty;

        public string MachineNo { get; set; } = String.Empty;

        public string Grade { get; set; } = String.Empty;

        public string UseYN { get; set; } = "Y";

        public string Vendor { get; set; } = String.Empty;

        public decimal Qty { get; set; } = Decimal.Zero;

        public decimal Price { get; set; } = Decimal.Zero;

        public string PurDate { get; set; } = String.Empty;

        public string InDate { get; set; } = String.Empty;

        public string ReplaceDate { get; set; } = String.Empty;

        public string DiscardDate { get; set; } = String.Empty;

        public string Remarks { get; set; } = String.Empty;

        public string Status { get; set; } = String.Empty;
    }
}
