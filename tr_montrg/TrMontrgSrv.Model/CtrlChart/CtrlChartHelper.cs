using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model.CtrlChart
{
    public class CtrlChartHelper
    {
        #region Properties

        public bool IsToCheckNelsonRule1 { get; set; }

        public bool IsToCheckNelsonRule2 { get; set; }

        public bool IsToCheckNelsonRule3 { get; set; }

        public bool IsToCheckNelsonRule4 { get; set; }

        public bool IsToCheckNelsonRule5 { get; set; }

        public bool IsToCheckNelsonRule6 { get; set; }

        public bool IsToCheckNelsonRule7 { get; set; }

        public bool IsToCheckNelsonRule8 { get; set; }

        public bool IsAutoUclLcl { get; set; }

        public double? UCL { get; set; }

        public double? LCL { get; set; }

        public double? CL { get; set; }

        #endregion

        #region Public Methods

        public void CheckNelsonRules(CtrlChartData data)
        {
            data.ViolatedNelsonRules.Clear();

            for (int i = 0; i < data.Values.Count; i++)
            {
                if (IsToCheckNelsonRule1 == true)
                {
                    if (IsAgainstNelsonRule1(data.UCL, data.LCL, data.Values[i]) == true)
                    {
                        data.ViolatedNelsonRules.Add(1);
                        continue;
                    }
                }

                if (IsToCheckNelsonRule2 == true)
                {
                    List<double> lst = data.Values as List<double>;
                    if (IsAgainstNelsonRule2(data.UCL, data.CL, data.LCL, lst.GetRange(0, i + 1).ToArray()) == true)
                    {
                        data.ViolatedNelsonRules.Add(2);
                        continue;
                    }
                }

                if (IsToCheckNelsonRule3 == true)
                {
                    if (IsAgainstNelsonRule3(data.Values.ToArray()) == true)
                    {
                        data.ViolatedNelsonRules.Add(3);
                        continue;
                    }
                }

                if (IsToCheckNelsonRule4 == true)
                {
                    if (IsAgainstNelsonRule4(data.Values.ToArray()) == true)
                    {
                        data.ViolatedNelsonRules.Add(4);
                        continue;
                    }
                }

                data.ViolatedNelsonRules.Add(0);
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="defectCounts">defect T/F of each board</param>
        /// <param name="subGroup">Subgroup Count</param>
        /// <returns></returns>
        public CtrlChartData CalcPCtrlChartData(char[] results, int subgroup)
        {
            if (results.Count() < subgroup)
                return new CtrlChartData();

            CtrlChartData data = new();

            if (subgroup == 0)
                return data;

            double totDefects = 0.0;

            int countOfdefect = 0;
            int cnt = 0;
            foreach (char r in results)
            {
                cnt++;
                if (r == 'N')
                {
                    countOfdefect++;
                    totDefects += 1.0;
                }

                if (subgroup == cnt)
                {
                    data.Values.Add(((double)countOfdefect / (double)cnt));
                    data.Defects.Add(countOfdefect);
                    data.ViolatedNelsonRules.Add(0);

                    cnt = 0;
                    countOfdefect = 0;
                }
            }

            double totInsps = (double)(data.Values.Count * subgroup);
            double pBar = totDefects / totInsps;
            double stdev = Math.Sqrt((pBar * (1 - pBar)) / subgroup);

            data.CL = pBar;
            if (IsAutoUclLcl == false && (UCL.HasValue && LCL.HasValue))
            {
                data.LCL = LCL.Value;
                data.UCL = UCL.Value;
            }
            else
            {
                data.UCL = pBar + 3 * stdev;
                data.LCL = (pBar - 3 * stdev) < 0 ? 0 : (pBar - 3 * stdev);
            }

            CheckNelsonRules(data);

            return data;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="defectCounts">defect T/F of each board</param>
        /// <param name="subGroup">Subgroup Count</param>
        /// <returns></returns>
        public CtrlChartData CalcUCtrlChartData(int[] defects, int subgroup)
        {
            if (defects.Count() < subgroup)
                return new CtrlChartData();

            CtrlChartData data = new();

            if (subgroup == 0)
                return data;

            double totDefects = 0.0;

            int countOfdefect = 0;
            int cnt = 0;
            foreach (int r in defects)
            {
                cnt++;
                //if (r == 'N')
                {
                    countOfdefect += r;
                    totDefects += r;
                }

                if (subgroup == cnt)
                {
                    data.Values.Add(((double)countOfdefect / (double)cnt));
                    data.Defects.Add(countOfdefect);
                    data.ViolatedNelsonRules.Add(0);

                    cnt = 0;
                    countOfdefect = 0;
                }
            }

            double totInsps = (double)(data.Values.Count * subgroup);
            double uBar = totDefects / totInsps;
            double stdev = Math.Sqrt(uBar / subgroup);

            data.CL = uBar;
            if (IsAutoUclLcl == false && (UCL.HasValue && LCL.HasValue))
            {
                data.LCL = LCL.Value;
                data.UCL = UCL.Value;
            }
            else
            {
                data.UCL = uBar + 3 * stdev;
                data.LCL = (uBar - 3 * stdev) < 0 ? 0 : (uBar - 3 * stdev);
            }

            CheckNelsonRules(data);

            return data;
        }

        public CtrlChartData CalcXbarOfXbarR(float?[] values, int subgroup)
        {
            if (values.Length < subgroup)
                return new CtrlChartData();

            GetXbarRResult(values, subgroup, out List<double> xbarList, out List<double> RList);

            double A2 = CtrlCoefficients.Instance.GetA2(subgroup);
            double Rbar = RList.Average();

            CtrlChartData result = new();
            result.ViolatedNelsonRules.AddRange(new int[xbarList.Count]);

            result.CL = (double)values.Average();
            result.LCL = result.CL - A2 * Rbar;
            result.UCL = result.CL + A2 * Rbar;
            result.Values = xbarList;
            result.SubgroupSize = subgroup;

            CheckNelsonRules(result);

            return result;
        }

        public CtrlChartData CalcROfXbarR(float?[] values, int subgroup)
        {
            if (values.Length < subgroup)
                return new CtrlChartData();

            GetXbarRResult(values, subgroup, out List<double> xbarList, out List<double> RList);

            double D4 = CtrlCoefficients.Instance.GetD4(subgroup);
            double D3 = CtrlCoefficients.Instance.GetD3(subgroup);
            double Rbar = RList.Average();

            CtrlChartData result = new();
            result.ViolatedNelsonRules.AddRange(new int[xbarList.Count]);

            result.CL = Rbar;
            result.LCL = D3 * Rbar;
            result.UCL = D4 * Rbar;
            result.Values = RList;

            CheckNelsonRules(result);

            return result;
        }

        public CtrlChartData CalcXbarOfXbarS(float?[] values, int subgroup)
        {
            if (values.Length < subgroup)
                return new CtrlChartData();


            GetXbarSResult(values, subgroup, out List<double> xbarList, out List<double> SList);

            double A3 = CtrlCoefficients.Instance.GetA3(subgroup);
            double Sbar = SList.Average();

            CtrlChartData result = new();
            result.ViolatedNelsonRules.AddRange(new int[xbarList.Count]);

            result.CL = (double)values.Average();
            result.LCL = result.CL - A3 * Sbar;
            result.UCL = result.CL + A3 * Sbar;
            result.Values = xbarList;
            result.SubgroupSize = subgroup;

            CheckNelsonRules(result);

            return result;
        }

        public CtrlChartData CalcSOfXbarS(float?[] values, int subgroup)
        {
            if (values.Length < subgroup)
                return new CtrlChartData();


            GetXbarSResult(values, subgroup, out List<double> xbarList, out List<double> SList);

            double B4 = CtrlCoefficients.Instance.GetB4(subgroup);
            double B3 = CtrlCoefficients.Instance.GetB3(subgroup);
            double Sbar = SList.Average();

            CtrlChartData result = new();
            result.ViolatedNelsonRules.AddRange(new int[xbarList.Count]);

            result.CL = Sbar;
            result.LCL = B3 * Sbar;
            result.UCL = B4 * Sbar;
            result.Values = SList;

            CheckNelsonRules(result);

            return result;
        }

        /// <summary>
        /// IMR Control Chart = X-Rs Control Chart
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public CtrlChartData CalcIOfIMR(float?[] values)
        {
            if (values.Length == 0)
                return new CtrlChartData();

            double MR = (double)values.Max() - (double)values.Min();
            double d2 = CtrlCoefficients.Instance.Getd2(2);

            GetIMRResult(values, 2, out List<double> MRList);

            CtrlChartData result = new();
            result.ViolatedNelsonRules.AddRange(new int[values.Length]);

            result.CL = (double)values.Average();
            result.UCL = result.CL + 3 * MRList.Average() / d2;
            result.LCL = result.CL - 3 * MRList.Average() / d2;
            result.Values = values.Select(x => (double)x.Value).ToList();
            result.MrValues = MRList;

            CheckNelsonRules(result);

            return result;
        }

        public CtrlChartData CalcMROfIMR(float?[] values)
        {
            if (values.Length == 0)
                return new CtrlChartData();

            double MR = (double)values.Max() - (double)values.Min();
            double D4 = CtrlCoefficients.Instance.GetD4(2);
            double D3 = CtrlCoefficients.Instance.GetD3(2);

            GetIMRResult(values, 2, out List<double> MRList);

            CtrlChartData result = new();
            result.ViolatedNelsonRules.AddRange(new int[values.Length]);

            result.CL = MRList.Average();
            result.UCL = MRList.Average() * D4;
            result.LCL = MRList.Average() * D3;
            result.Values = MRList;

            CheckNelsonRules(result);

            return result;
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Check whether one point is between UCL and LCL.
        /// </summary>
        /// <remarks>
        /// Nelson rule 1: One point is more than 3 standard deviations(3 sigma or UCL/LCL) from the mean (x-bar).
        /// 1 stdev = 1 sigma
        /// UCL/LCL = 3 sigma
        /// </remarks>
        /// <param name="ucl">UCL value</param>
        /// <param name="lcl">LCL value</param>
        /// <param name="result">Point</param>
        /// <returns>True if the specified point is against the rule, otherwise false</returns>
        private static bool IsAgainstNelsonRule1(double ucl, double lcl, double result)
        {
            if (ucl < result || lcl > result)
                return true;

            return false;
        }

        /// <summary>
        /// Check whether countinuous nine points are bias on the same side of CL.
        /// </summary>
        /// <remarks>
        /// Nelson rule 2: Nine (or more) points in a row are on the same side of the mean.
        /// </remarks>
        /// <param name="ucl">UCL value</param>
        /// <param name="cl">CL value</param>
        /// <param name="lcl">LCL value</param>
        /// <param name="results">Point list</param>
        /// <returns>True if the specified point is against the rule, otherwise false</returns>
        private static bool IsAgainstNelsonRule2(double ucl, double cl, double lcl, double[] results)
        {
            bool isAgainst = false;

            if (results.Length < 9)
                return isAgainst;

            int idx = results.Length;
            if (((cl < results[idx - 1] && ucl > results[idx - 1]) &&
                 (cl < results[idx - 2] && ucl > results[idx - 2]) &&
                 (cl < results[idx - 3] && ucl > results[idx - 3]) &&
                 (cl < results[idx - 4] && ucl > results[idx - 4]) &&
                 (cl < results[idx - 5] && ucl > results[idx - 5]) &&
                 (cl < results[idx - 6] && ucl > results[idx - 6]) &&
                 (cl < results[idx - 7] && ucl > results[idx - 7]) &&
                 (cl < results[idx - 8] && ucl > results[idx - 8]) &&
                 (cl < results[idx - 9] && ucl > results[idx - 9])) ||

                ((cl > results[idx - 1] && lcl < results[idx - 1]) &&
                 (cl > results[idx - 2] && lcl < results[idx - 2]) &&
                 (cl > results[idx - 3] && lcl < results[idx - 3]) &&
                 (cl > results[idx - 4] && lcl < results[idx - 4]) &&
                 (cl > results[idx - 5] && lcl < results[idx - 5]) &&
                 (cl > results[idx - 6] && lcl < results[idx - 6]) &&
                 (cl > results[idx - 7] && lcl < results[idx - 7]) &&
                 (cl > results[idx - 8] && lcl < results[idx - 8]) &&
                 (cl > results[idx - 9] && lcl < results[idx - 9])))
            return true;

            //int upper = 0, lower = 0;

            //// Always loop last 9 items of the list
            //for (int i = results.Length - 1; i > results.Length - 10; i--)
            //{
            //   if (cl < results[i] && ucl > results[i])
            //      upper++;

            //   if (cl > results[i] && lcl < results[i])
            //      lower++;
            //}

            //if (upper >= 9 || lower >= 9)
            //   isAgainst = true;

            return isAgainst;
        }

        /// <summary>
        /// Check whether six points increase/decrease continually.
        /// </summary>
        /// <remarks>
        /// Nelson rule 3: Six (or more) points in a row are continually increasing or decreasing.
        /// </remarks>
        /// <param name="results">Point list</param>
        /// <returns>True if the specified point is against the rule, otherwise false</returns>
        private static bool IsAgainstNelsonRule3(double[] results)
        {
            bool isAgainst = false;

            if (results.Length < 6)
                return isAgainst;

            int idx = results.Length;
            if ((results[idx - 1] > results[idx - 2] &&
                 results[idx - 2] > results[idx - 3] &&
                 results[idx - 3] > results[idx - 4] &&
                 results[idx - 4] > results[idx - 5] &&
                 results[idx - 5] > results[idx - 6]) ||

                (results[idx - 1] < results[idx - 2] &&
                 results[idx - 2] < results[idx - 3] &&
                 results[idx - 3] < results[idx - 4] &&
                 results[idx - 4] < results[idx - 5] &&
                 results[idx - 5] < results[idx - 6]))
            {
                return true;
            }

            //int increase = 0, decrease = 0;

            //for (int i = results.Length - 1; i > results.Length - 6; i--)
            //{
            //   if (results[i] > results[i - 1])
            //      increase++;

            //   if (results[i] < results[i - 1])
            //      decrease++;

            //   if (increase >= 6 || decrease >= 6)
            //      isAgainst = true;
            //}

            return isAgainst;
        }

        /// <summary>
        /// Check whether fourteen (or more) points in a row alternate in direction, increasing then decreasing.
        /// </summary>
        /// <remarks>
        /// The position of mean and size of standard deviation do not affect this rule.
        /// </remarks>
        /// <param name="results">Point list</param>
        /// <returns>True if the specified point is against the rule, otherwise false</returns>
        private static bool IsAgainstNelsonRule4(double[] results)
        {
            bool isAgainst = false;

            if (results.Length < 14)
                return false;

            int idx = results.Length;
            if ((results[idx - 1] < results[idx - 2] &&
                 results[idx - 2] > results[idx - 3] &&
                 results[idx - 3] < results[idx - 4] &&
                 results[idx - 4] > results[idx - 5] &&
                 results[idx - 5] < results[idx - 6] &&
                 results[idx - 6] > results[idx - 7] &&
                 results[idx - 7] < results[idx - 8] &&
                 results[idx - 8] > results[idx - 9] &&
                 results[idx - 9] < results[idx - 10] &&
                 results[idx - 10] > results[idx - 11] &&
                 results[idx - 11] < results[idx - 12] &&
                 results[idx - 12] > results[idx - 13] &&
                 results[idx - 13] < results[idx - 14]) ||
                (results[idx - 1] > results[idx - 2] &&
                 results[idx - 2] < results[idx - 3] &&
                 results[idx - 3] > results[idx - 4] &&
                 results[idx - 4] < results[idx - 5] &&
                 results[idx - 5] > results[idx - 6] &&
                 results[idx - 6] < results[idx - 7] &&
                 results[idx - 7] > results[idx - 8] &&
                 results[idx - 8] < results[idx - 9] &&
                 results[idx - 9] > results[idx - 10] &&
                 results[idx - 10] < results[idx - 11] &&
                 results[idx - 11] > results[idx - 12] &&
                 results[idx - 12] < results[idx - 13] &&
                 results[idx - 13] > results[idx - 14]))
            {
                return true;
            }

            //bool? flag = null;

            //for (int i = results.Length - 1; i > results.Length - 15; i--)
            //{
            //   bool? prev = flag;

            //   if (results[i] > results[i - 1])
            //      flag = true;
            //   else if (results[i] < results[i - 1])
            //      flag = false;
            //   else
            //      flag = prev;

            //   if (prev == flag)
            //   {
            //      isAgainst = false;
            //      break;
            //   }
            //}

            return isAgainst;
        }

        private static void GetXbarRResult(float?[] values, int subGroup, out List<double> xbarList, out List<double> RList)
        {
            xbarList = new List<double>();
            RList = new List<double>();

            int n = 0;
            double avg = 0;
            double max = double.MinValue;
            double min = double.MaxValue;

            foreach (float? x in values)
            {
                n++;

                max = System.Math.Max((double)x.Value, max);
                min = System.Math.Min((double)x.Value, min);

                avg += Convert.ToDouble(x.Value);
                if (n == subGroup)
                {
                    xbarList.Add(avg / (double)subGroup);
                    RList.Add(max - min);

                    n = 0;
                    avg = 0.0;

                    max = double.MinValue;
                    min = double.MaxValue;
                }
            }
        }

        private static void GetXbarSResult(float?[] values, int subGroup, out List<double> xbarList, out List<double> SList)
        {
            xbarList = new List<double>();
            SList = new List<double>();

            int n = 0;
            double avg = 0;

            List<double> tmp = new();

            foreach (float? x in values)
            {
                n++;

                avg += Convert.ToDouble(x.Value);
                tmp.Add(Convert.ToDouble(x.Value));
                if (n == subGroup)
                {
                    xbarList.Add(avg / (double)subGroup);

                    n = 0;
                    avg = 0.0;

                    //SList.Add(StatisticsHelper.StdDev(tmp.ToArray(), tmp.Count));
                    SList.Add(StatisticsHelper.StdDev(tmp.ToArray()));
                    tmp.Clear();
                }
            }
        }

        private static void GetIMRResult(float?[] values, int subGroup, out List<double> MRList)
        {
            MRList = new List<double>();

            int n = 0;
            List<double> tmp = new();

            foreach (float? x in values)
            {
                n++;

                tmp.Add(Convert.ToDouble(x.Value));
                if (n == subGroup)
                {
                    MRList.Add(tmp.Max() - tmp.Min());

                    n = tmp.Count - 1;
                    tmp.RemoveAt(0);
                }
            }
        }

        #endregion
    }
}
