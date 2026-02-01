namespace CSG.MI.DAO.Production.PCC
{
    public class ProdMaterial : BaseModel
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public string InStatus { get; set; } = String.Empty;

        public string MatStart { get; set; } = String.Empty;

        public string MatEnd { get; set; } = String.Empty;

        public string MatStatus { get; set; } = String.Empty;

        public string LamDate { get; set; } = String.Empty;

        public string LamStatus { get; set; } = String.Empty;

        public string SwatchDate { get; set; } = String.Empty;

        public string SwatchStatus { get; set; } = String.Empty;

        public string WsError { get; set; } = String.Empty;

        public string WsCancel { get; set; } = String.Empty;

        public string TracingPapper { get; set; } = String.Empty;
    }
}
