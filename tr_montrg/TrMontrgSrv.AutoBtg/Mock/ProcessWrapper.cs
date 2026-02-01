using CSG.MI.TrMontrgSrv.AutoBtg.Mock.Interface;
using System.Diagnostics;

namespace CSG.MI.TrMontrgSrv.AutoBtg.Mock
{
    public class ProcessWrapper : IProcessWrapper
    {
        private readonly Process _process;

        public ProcessWrapper(Process process)
        {
            _process = process ?? throw new ArgumentNullException(nameof(process));
        }

        public void WaitForExit() => _process.WaitForExit();

        public bool HasExited => _process.HasExited;
    }
}
