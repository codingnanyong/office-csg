namespace CSG.MI.DAO.Production.RTLS
{
    public class EslLocation : BaseModel
    {
        public string BeaconTagId { get; set; } = String.Empty;

        public string ProcessLoc { get; set; } = String.Empty;

        public DateTime UpdateDt { get; set; } = DateTime.MaxValue;
    }
}