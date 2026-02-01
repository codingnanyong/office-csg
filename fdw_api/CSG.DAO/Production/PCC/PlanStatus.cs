namespace CSG.MI.DAO.Production.PCC
{
    public class PlanStatus : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public decimal Seq { get; set; } = Decimal.Zero;

        public string WsStatus { get; set; } = String.Empty;

        public DateTime RequestDate { get; set; } = DateTime.Now;

        public string Status { get; set; } = String.Empty;

        public DateTime PlnaDate { get; set; } = DateTime.Now;

        public string Remark { get; set; } = String.Empty;

        public string EmerYN { get; set; } = String.Empty;

    }
}
