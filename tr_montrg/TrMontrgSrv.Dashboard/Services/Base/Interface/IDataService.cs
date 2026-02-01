namespace CSG.MI.TrMontrgSrv.Dashboard.Services.Base.Interface
{
    public interface IDataService
    {
        string Host { get; }

        float Version { get; }

        HttpClient HttpClient { get; }
    }
}
