using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model.CtrlChart
{
    public class CtrlChartData
    {
        public double UCL { get; set; }
        public double CL { get; set; }
        public double LCL { get; set; }

        public IList<double> Values { get; set; } = new List<double>();

        public IList<int> Defects { get; set; } = new List<int>();

        // It is only used for IMR Control Chart
        public IList<double> MrValues { get; set; } = new List<double>();

        /// <summary>
        /// Add the Nelson rule number if values are against a Nelson Rule, and
        /// add 0 if no violation of the rules.
        /// Total length of returned list will be same as data size.
        /// </summary>
        public List<int> ViolatedNelsonRules { get; set; } = new List<int>();

        public int SubgroupSize { get; set; }
    }
}
