namespace CSG.MI.DAO.Production.PCC
{
    public class IssueMst : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string IssueCd { get; set; } = String.Empty;

        public decimal IssueLevel { get; set; } = Decimal.Zero;

        public string IssueName { get; set; } = String.Empty;

        public string ParentIssueCd { get; set; } = String.Empty;

        public string UseYN { get; set; } = String.Empty;
    }
}
