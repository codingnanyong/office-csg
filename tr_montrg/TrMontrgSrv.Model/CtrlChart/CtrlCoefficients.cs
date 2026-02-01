using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model.CtrlChart
{
    public class CtrlCoefficients
    {
        #region Fields

        private static CtrlCoefficients _instance;
        private const double k = 3.0;
        private readonly int MAX_SUBGROUP_SIZE = 26;

        /// <summary>
        /// d2, d3, c4 control chart coefficients table
        /// </summary>
        private readonly double[] d2 = { 1, 1.128, 1.693, 2.059, 2.326, 2.534, 2.704, 2.847, 2.97, 3.078, 3.173, 3.258, 3.336, 3.407, 3.472, 3.532, 3.588, 3.64, 3.689, 3.735, 3.778, 3.819, 3.858, 3.895, 3.931, 3.965, 3.997, 4.028, 4.058, 4.086, 4.113, 4.139, 4.164, 4.189, 4.213, 4.236, 4.258, 4.28, 4.301, 4.322, 4.342, 4.361, 4.38, 4.398, 4.415, 4.432, 4.449, 4.466, 4.482, 4.498 };
        private readonly double[] d3 = { 0.82, 0.8525, 0.8884, 0.8794, 0.8641, 0.848, 0.8332, 0.8198, 0.8078, 0.7971, 0.7873, 0.7785, 0.7704, 0.763, 0.7562, 0.7499, 0.7441, 0.7386, 0.7335, 0.7287, 0.7242, 0.7199, 0.7159, 0.7121, 0.7084 };
        private readonly double[] c4 = { 0, 0.797885, 0.886227, 0.921318, 0.939986, 0.951533, 0.959369, 0.96503, 0.969311, 0.972659, 0.97535, 0.977559, 0.979406, 0.980971, 0.982316, 0.983484, 0.984506, 0.98541, 0.986214, 0.986934, 0.987583, 0.98817, 0.988705, 0.989193, 0.98964, 0.990052, 0.990433, 0.990786, 0.991113, 0.991418, 0.991703, 0.991969, 0.992219, 0.992454, 0.992675, 0.992884, 0.99308, 0.993267, 0.993443, 0.993611, 0.99377, 0.993922, 0.994066, 0.994203, 0.994335, 0.99446, 0.99458, 0.994695, 0.994806, 0.994911 };
        // For c4 Approximation for n > 50 c4 = 4(n-1)/(4n -3)

        #endregion

        #region Public Methods

        /// <summary>
        /// Get d2 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double Getc4(int idx)
        {
            if (MAX_SUBGROUP_SIZE <= idx)
            {
                return (4.0 * ((double)idx - 1.0)) / (4.0 * (double)idx - 3.0);
            }

            return c4[idx - 1];
        }

        /// <summary>
        /// Get d2 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double Getd2(int idx)
        {
            if (MAX_SUBGROUP_SIZE <= idx)
                return 0;

            return d2[idx - 1];
        }

        /// <summary>
        /// Get d3 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double Getd3(int idx)
        {
            if (MAX_SUBGROUP_SIZE <= idx)
                return 0;

            return d3[idx - 1];
        }

        /// <summary>
        /// Get A2 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double GetA2(int idx)
        {
            if (MAX_SUBGROUP_SIZE <= idx)
                return 0;

            return k / (d2[idx - 1] * Math.Sqrt(idx));
        }

        /// <summary>
        /// Get A3 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double GetA3(int idx)
        {
            if (MAX_SUBGROUP_SIZE <= idx)
                return k / (Getc4(idx) * Math.Sqrt(idx));

            return k / (Getc4(idx) * Math.Sqrt(idx));
        }

        /// <summary>
        /// Get B3 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double GetB3(int idx)
        {
            //if (MAX_SUBGROUP_SIZE <= idx)
            //	return Math.Max(0, 1 - (k / 1) * Math.Sqrt(1 - Math.Pow(1, 2))); ;

            return Math.Max(0, 1 - (k / Getc4(idx)) * Math.Sqrt(1 - Math.Pow(Getc4(idx), 2)));
        }

        /// <summary>
        /// Get B4 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double GetB4(int idx)
        {
            //if (MAX_SUBGROUP_SIZE <= idx)
            //   return (1 + (k / 1) * Math.Sqrt(1 - Math.Pow(1, 2))); ;

            return (1 + (k / Getc4(idx)) * Math.Sqrt(1 - Math.Pow(Getc4(idx), 2)));
        }

        /// <summary>
        /// Get D3 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double GetD3(int idx)
        {
            if (MAX_SUBGROUP_SIZE <= idx)
                return 0;

            return Math.Max(0, 1 - k * d3[idx - 1] / d2[idx - 1]);
        }

        /// <summary>
        /// Get D4 control chart coefficients table
        /// </summary>
        /// <param name="idx">subgroup size</param>
        /// <returns></returns>
        public double GetD4(int idx)
        {
            if (MAX_SUBGROUP_SIZE <= idx)
                return 0;

            return 1 + k * d3[idx - 1] / d2[idx - 1];
        }

        #endregion

        #region Singleton

        public static CtrlCoefficients Instance
        {
            get
            {
                if (_instance == null)
                    _instance = new CtrlCoefficients();

                return _instance;
            }
        }

        #endregion
    }
}
