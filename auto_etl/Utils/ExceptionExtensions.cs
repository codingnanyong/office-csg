//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using System;
using System.Net;
using System.Text;

namespace CSG.MI.AutoETL.Utils
{
	public static class ExceptionExtensions
	{
		public static string ToFormattedString(this Exception exception)
		{
			StringBuilder sb = new StringBuilder();

			sb.AppendFormat("Date: {0}{1}", DateTime.Now.ToString("u"), Environment.NewLine);
			sb.AppendFormat("Computer: {0}{1}", Dns.GetHostName(), Environment.NewLine);
			sb.AppendFormat("Source: {0}{1}", (exception.Source != null) ? exception.Source.Trim() : null, Environment.NewLine);
			sb.AppendFormat("Method: {0}{1}", (exception.TargetSite != null) ? exception.TargetSite.Name : null, Environment.NewLine);
			sb.AppendFormat("Message: {0}{1}", (exception.Message != null) ? exception.Message.TrimEnd(new char[] { '\r','\n' }) : null, Environment.NewLine);
			sb.AppendFormat("Stack Trace: {1}{0}", (exception.StackTrace != null) ? exception.StackTrace.TrimEnd(new char[] { '\r', '\n' }) : null, Environment.NewLine);

			return sb.ToString();
		}
	}
}
