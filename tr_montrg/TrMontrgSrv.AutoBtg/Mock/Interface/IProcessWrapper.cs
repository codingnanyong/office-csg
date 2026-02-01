namespace CSG.MI.TrMontrgSrv.AutoBtg.Mock.Interface
{
    public interface IProcessWrapper
    {
        void WaitForExit();
        bool HasExited { get; }
    }
}
