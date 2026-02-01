namespace CSG.MI.DAO.Production.PCC
{
    public class ReworkHead : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public decimal Seq { get; set; } = Decimal.Zero;

        public string OpCd { get; set; } = String.Empty;

        public string OccurDate { get; set; } = String.Empty;

        public string Remark { get; set; } = String.Empty;

        public string CompleteDate { get; set; } = String.Empty;

        public string Status { get; set; } = String.Empty;
    }
}
