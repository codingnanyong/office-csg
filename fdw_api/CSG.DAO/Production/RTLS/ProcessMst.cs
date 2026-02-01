namespace CSG.MI.DAO.Production.RTLS
{
    public class ProcessMst : BaseModel
    {
        public string ProcessLoc { get; set; } = String.Empty;

        public string ProcessLocName { get; set; } = String.Empty;

        public string LocName { get; set; } = String.Empty;

        public string StepCode { get; set; } = String.Empty;

        public string Level1 { get; set; } = String.Empty;

        public string Level2 { get; set; } = String.Empty;
    }
}
