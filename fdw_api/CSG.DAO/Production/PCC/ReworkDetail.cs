namespace CSG.MI.DAO.Production.PCC
{
    public class ReworkDetail : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public decimal Seq { get; set; } = Decimal.Zero;

        public decimal PartSeq { get; set; } = Decimal.Zero;

        public decimal LamSeq { get; set; } = Decimal.Zero;

        public string Remark { get; set; } = String.Empty;
    }
}
