using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Fsw
{
	/// <summary>
	///
	/// </summary>
	public static class Utility
	{
		/// <summary>
		///
		/// </summary>
		/// <param name="path"></param>
		/// <returns></returns>
		public static bool IsPathAvailable(string path)
		{
			if (Directory.Exists(path) == false)
				return false;

			return true;
		}
	}
}
