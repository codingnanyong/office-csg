namespace CSG.MI.DAO.Production.PCC
{
    public class PlanOp : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public decimal Seq { get; set; } = Decimal.Zero;

        public string OpCd { get; set; } = String.Empty;

        public decimal Qty { get; set; } = Decimal.Zero;

        public string Chk { get; set; } = String.Empty;

        public string Date { get; set; } = String.Empty;

        public string Passcard { get; set; } = String.Empty;

        public string ReworkStat { get; set; } = String.Empty;
    }
}
