using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Management;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Helpers
{
    /// <summary>
    ///
    /// </summary>
    public class ProcessHelper
    {
        /// <summary>
        /// Checks whether the specified process is running.
        /// </summary>
        /// <param name="name">Process name to check</param>
        /// <returns>True if exists, otherwise false</returns>
        public static bool ExistsAnyProcess(string name)
        {
            return GetProcesses(name).Length > 0;
        }

        /// <summary>
        /// Gets all processes by the specified name.
        /// </summary>
        /// <param name="name">Process name to find</param>
        /// <returns>Array of the processes found</returns>
        public static Process[] GetProcesses(string name)
        {
            Process[] processes = Process.GetProcessesByName(name);
            return processes;
        }

        /// <summary>
        /// Gets all processes having the same name as the current running process.
        /// </summary>
        /// <returns>Array of the processes found</returns>
        public static Process[] GetOtherSameProcesses()
        {
            Process currentProcess = Process.GetCurrentProcess();
            Process[] processes = Process.GetProcessesByName(currentProcess.ProcessName);

            // [2017.12.13 jwlee3]
            // return processes.Where(s => s.StartTime < Process.GetCurrentProcess().StartTime).ToArray();

            var list = new List<Process>();

            foreach (var process in processes)
            {
                var startTime = ProcessHelper.GetSafeStartTime(process);
                if (startTime == null)
                {
                    continue;
                }

                var startTimeCurrProcess = ProcessHelper.GetSafeStartTime(currentProcess);
                if (startTimeCurrProcess == null)
                {
                    continue;
                }

                if (startTime.Value < startTimeCurrProcess.Value)
                {
                    list.Add(process);
                }
            }

            return list.ToArray();
        }

#pragma warning disable CA1416 // Validate platform compatibility
        /// <summary>
        /// Safe version of Process.StartTime because Process.StartTime is for full trust for the immediate caller. This member cannot be used by partially trusted code.
        /// </summary>
        /// <param name="process">Process to get its start time</param>
        /// <returns>Process start time</returns>
        /// <remarks>
        /// If you make sure that the user has the PROCESS_QUERY_LIMITED_INFORMATION access right, that should be enough to fix the Access denied exception.
        /// However, if you don't have control over the access rights, you should try to use WMI (Windows Management Instrumentation) to get the process information.
        /// https://reformatcode.com/code/c/access-denied-on-windows-forms-application-fetching-processstarttime
        ///
        /// MSDN: Process.StartTime = for full trust for the immediate caller. This member cannot be used by partially trusted code.
        /// https://msdn.microsoft.com/query/dev14.query?appId=Dev14IDEF1&l=EN-US&k=k(System.Diagnostics.Process.StartTime);k(TargetFrameworkMoniker-.NETFramework,Version%3Dv4.6);k(DevLang-csharp)&rd=true
        ///
        /// Solution code sample: https://stackoverflow.com/questions/28708/process-starttime-access-denied
        /// </remarks>
        public static DateTime? GetSafeStartTime(Process process)
        {
            int processId = process.Id;
            string wmiQueryString = String.Format("SELECT CreationDate FROM Win32_Process WHERE ProcessId='{0}'", processId);
            SelectQuery query = new(wmiQueryString);

            ManagementScope scope = new(@"\\.\root\CIMV2");
            ManagementObjectSearcher searcher = new(scope, query);
            ManagementObjectCollection processes = searcher.Get();

            foreach (var p in processes)
            {
                DateTime startTime = ManagementDateTimeConverter.ToDateTime(p["CreationDate"].ToString());
                return startTime;
            }

            return null;
        }
#pragma warning restore CA1416 // Validate platform compatibility

        /// <summary>
        /// Get total number of all same processes having the same name as the current running process.
        /// </summary>
        /// <returns>Total number of the same processes</returns>
        public static int TotalOtherSameProcesses()
        {
            Process[] processes = GetOtherSameProcesses();
            int total = processes.Length;

            return total;
        }

        /// <summary>
        /// Kills the specified process.
        /// </summary>
        /// <param name="name">Process name to kill</param>
        /// <returns>True if killed, otherwise false</returns>
        public static bool KillProcesses(string name)
        {
            foreach (Process process in GetProcesses(name))
            {
                process.Kill();
            }

            return !ExistsAnyProcess(name);
        }
#pragma warning disable CA1416 // Validate platform compatibility
        /// <summary>
        /// Gets all processes with path and command line.
        /// </summary>
        /// <returns>List of processes with path and command line</returns>
        public static List<Tuple<Process, string, string>> GetProcessesWithExecutablePath()
        {
            var list = new List<Tuple<Process, string, string>>();

            var wmiQueryString = "SELECT ProcessId, ExecutablePath, CommandLine FROM Win32_Process";

            using (ManagementObjectSearcher searcher = new(queryString: wmiQueryString))
            using (ManagementObjectCollection results = searcher.Get())
            {
                var query = from p in Process.GetProcesses()
                            join mo in results.Cast<ManagementObject>()
                            on p.Id equals (int)(uint)mo[propertyName: "ProcessId"]
                            select new
                            {
                                Process = p,
                                Path = (string)mo["ExecutablePath"],
                                CommandLine = (string)mo["CommandLine"],
                            };

                foreach (var item in query)
                {
                    var tuple = new Tuple<Process, string, string>(item.Process, item.Path, item.CommandLine);
                    list.Add(tuple);
                }
            }

            return list;
        }
#pragma warning restore CA1416 // Validate platform compatibility
    }
}
