using CSG.MI.TrMontrgSrv.AutoBtg.PingCore.Interface;
using System.Net.NetworkInformation;

namespace CSG.MI.TrMontrgSrv.AutoBtg.PingCore
{
    public class PingWrapper : IPingWrapper
    {
        /// <summary>
        /// IpAdress Ping Test
        /// </summary>
        /// <param name="ipAddress">IPv4 Address</param>
        /// <returns></returns>
        public PingReply Send(string ipAddress)
        {
            using (Ping ping = new Ping())
            {
                try
                {
                    return ping.Send(ipAddress);
                }
                catch (PingException ex)
                {
                    StaticLogger.Logger.LogError(ex.Message);
                    return null!;
                }
            }
        }
    }
}