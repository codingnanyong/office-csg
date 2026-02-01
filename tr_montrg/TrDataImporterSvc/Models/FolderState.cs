using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Models
{
	public enum FolderState
	{
		Unknown = -1,
		Monitoring = 0,
		NotMonitoring,
		PathNotAvailable
	}
}
