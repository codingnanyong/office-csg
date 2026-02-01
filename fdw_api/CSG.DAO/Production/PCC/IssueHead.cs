namespace CSG.MI.DAO.Production.PCC
{
    public class IssueHead : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public string OpCd { get; set; } = String.Empty;

        public decimal Seq { get; set; } = Decimal.Zero;

        public string Level1 { get; set; } = String.Empty;

        public string Level2 { get; set; } = String.Empty;

        public string Level3 { get; set; } = String.Empty;

        public string Status { get; set; } = String.Empty;

        public DateTime IssueDate { get; set; } = DateTime.Now;

        public DateTime CompleteDueDate { get; set; } = DateTime.Now;

        public DateTime CompleteDate { get; set; } = DateTime.Now;
    }
}
