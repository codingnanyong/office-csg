namespace CSG.MI.TrMontrgSrv.Model.Inspection
{
    public class IoTBatchFile : BaseModel
    {
        public string Name { get; set; } = "IoTBatch";

        public FileType fileType { get; set; } = 0;

        public string FileFullName { get; set; } = string.Empty;

        public string Command { get; set; } = string.Empty;

        public void SetFileName()
        {
            this.FileFullName = this.Name + "." + this.fileType.ToString();
        }

        public IoTBatchFile()
        {
            this.SetFileName();
        }
    }
}
