using System.Net;
using System.Text;

namespace CSG.MI.FDW.LoggerService
{
	public static class ExceptionExtensions
	{
		public static string ToFormattedString(this Exception exception)
		{
			StringBuilder sb = new();

			sb.AppendFormat("Date: {0}{1}", DateTime.Now.ToString("u"), Environment.NewLine);
			sb.AppendFormat("Computer: {0}{1}", Dns.GetHostName(), Environment.NewLine);
			sb.AppendFormat("Source: {0}{1}", exception.Source?.Trim(), Environment.NewLine);
			sb.AppendFormat("Method: {0}{1}", exception.TargetSite?.Name, Environment.NewLine);
			sb.AppendFormat("Message: {0}{1}", exception.Message?.TrimEnd(new char[] { '\r', '\n' }), Environment.NewLine);
			sb.AppendFormat("Stack Trace: {1}{0}", exception.StackTrace?.TrimEnd(new char[] { '\r', '\n' }), Environment.NewLine);

			return sb.ToString();
		}
	}
}
