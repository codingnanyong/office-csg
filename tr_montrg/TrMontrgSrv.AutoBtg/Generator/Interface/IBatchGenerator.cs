namespace CSG.MI.TrMontrgSrv.AutoBtg.Generator.Interface
{
    public interface IBatchGenerator<T> where T : class
    {
        void Run(T data);

        void CreateBatch();

        void WriteBatch(T data);

        void DeleteBatch();

        void ExecutionBatch();

        void AdminExecutionBatch();
    }
}
