using System.Net.NetworkInformation;

namespace CSG.MI.TrMontrgSrv.AutoBtg.PingCore.Interface
{
    public interface IPingWrapper
    {
        PingReply Send(string ipAddress);
    }
}