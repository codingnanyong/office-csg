namespace CSG.MI.DAO.Production.PCC
{
    public class ProdScan : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public string OpCd { get; set; } = String.Empty;

        public decimal ProdSeq { get; set; } = Decimal.Zero;

        public decimal Qty { get; set; } = Decimal.Zero;

        public string Ymd { get; set; } = String.Empty;

        public string Time { get; set; } = String.Empty;

        public string Status { get; set; } = String.Empty;

        public string ReworkStatus { get; set; } = String.Empty;
    }
}
