namespace CSG.MI.DAO.Production.PCC
{
    public class OpMst : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string OpCd { get; set; } = String.Empty;

        public string Name { get; set; } = String.Empty;

        public string LocalName { get; set; } = String.Empty;
        
        public decimal LeadTime { get; set; } = Decimal.Zero;

        public decimal SortNo { get; set; } = Decimal.Zero;

        public string Status { get; set; } = String.Empty;

        public decimal Capacity { get; set; } = Decimal.Zero;
    }
}
