using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IDataService
    {
        string Host { get; }

        int Version { get; }

        HttpClient HttpClient { get; }
    }
}
