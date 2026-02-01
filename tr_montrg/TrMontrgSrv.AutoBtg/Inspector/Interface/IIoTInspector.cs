namespace CSG.MI.TrMontrgSrv.AutoBtg.Inspector.Interface
{
    public interface IIoTInspector<T> where T : class
    {
        Task<ICollection<T>> GetDatas();

        void InspectionData(ICollection<T> datas);

        bool PingTest(T data);
    }
}
