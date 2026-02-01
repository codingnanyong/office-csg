using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model.CtrlChart
{
    public class StatisticsHelper
    {
        #region Average(Mean) of a set of data

        public static double ArithmeticMean(double[] data, int items)
        {
            int i;
            double mean, sum;

            sum = 0.0;

            for (i = 0; i < items; i++)
            {
                sum += data[i];
            }

            mean = sum / (double)items;

            return mean;
        }

        public static double ArithmeticMean(IEnumerable<object> data)
        {
            double mean;

            if (data.Count() == 0)
                return 0;

            mean = data.Average(d => (double)d);

            return mean;
        }

        public static double ArithmeticMean(IEnumerable<double> data)
        {
            double mean;

            if (data.Count() == 0)
                return 0;

            mean = data.Average(d => d);

            return mean;
        }

        #endregion

        // The geometric mean is similar to the arithmetic mean in that it is a type of average for the data,
        // except it has a rather different way of calculating it.
        // The geometric mean is defined as the n-th root of the product of each value in the data set,
        // where n is the number of data points in the set. This makes it useful for describing things
        // such as percentage growth.
        public static double GeometricMean(double[] data, int items)
        {
            int i;
            double geoMean, prod;

            prod = 1.0;

            for (i = 0; i < items; i++)
            {
                prod *= data[i];
            }

            geoMean = Math.Pow(prod, (1.0 / (double)items));

            return geoMean;
        }

        // The median is defined as the data point that falls exactly at the midpoint of a set of data points.
        // If there is an even number of points, then the average is taken of the middle two points
        public static double Median(double[] data, int items)
        {
            int midPoint;
            double median, sum;

            sum = 0.0;

            if (((int)Math.Round((double)items / 2.0) * 2) != items)
            {
                midPoint = items / 2;

                sum = data[midPoint];
                sum += data[midPoint + 1];
                sum /= 2.0;
            }
            else
            {
                midPoint = (items / 2) + 1;
                sum = data[midPoint];
            }

            median = sum;
            return median;
        }

        // 분산값을 구한다, 분산 = 표준편차(S)^2
        public static double Variance(double[] data, int items)
        {
            int i;
            double variance;
            double[] deviation = new double[items];

            double mean = ArithmeticMean(data, items);

            for (i = 0; i < items; i++)
            {
                deviation[i] = Math.Pow((data[i] - mean), 2);
            }

            variance = ArithmeticMean(deviation, items);

            return variance;
        }

        // 분산값을 구한다, 분산 = 표준편차(S)^2
        public static double Variance(IEnumerable<object> data)
        {
            double variance;

            double mean = ArithmeticMean(data);
            var deviation = data.Select(d => Math.Pow((double)d - mean, 2));

            variance = ArithmeticMean(deviation); //= deviation.Average(d => d);

            return variance;
        }

        // Standard deviation of the sample
        public static double StdDev(double[] data, int items)
        {
            int i;
            double stddev, mean, devMean;
            double[] deviation = new double[items];

            mean = ArithmeticMean(data, items);

            for (i = 0; i < items; i++)
            {
                deviation[i] = Math.Pow((data[i] - mean), 2);
            }

            //devMean = ArithmeticMean(deviation, items);
            devMean = ArithmeticMean(deviation, items);
            stddev = Math.Sqrt(devMean);

            return stddev;
        }

        // 전수검사 기준, All standard deviation, 표준편차(S)
        public static double StdDevByTotalInspection(IEnumerable<double> data)
        {
            double stddev, mean, devsum;
            int n = data.Count();

            if (n == 0)
                return 0;

            mean = ArithmeticMean(data);
            devsum = data.Sum(d => Math.Pow(d - mean, 2));
            // 표준편차 구할 때에 모집단이 샘플링 검사일 경우에는 모집단의 수(n)에서 1을 뺀 값으로 나누는 것이 맞다.
            // 하지만 우리는 샘플링검사가 아니라 전수검사 기준이기 때문에 모집단의 수(N)으로 나누는 것이 옳다.
            // by smbg11
            //stddev = Math.Sqrt(devsum / (n - 1));
            stddev = Math.Sqrt(devsum / (n));

            return stddev;
        }

        // 샘플링 검사 기준, Sample standard deviation, 표준편차(S)
        public static double StdDevBySamplingInspection(IEnumerable<double> data)
        {
            double stddev, mean, devsum;
            int n = data.Count();

            if (n == 0)
                return 0;

            mean = ArithmeticMean(data);
            devsum = data.Sum(d => Math.Pow(d - mean, 2));
            // 표준편차 구할 때에 모집단이 샘플링 검사일 경우에는 모집단의 수(n)에서 1을 뺀 값으로 나누는 것이 맞다.
            // 하지만 우리는 샘플링검사가 아니라 전수검사 기준이기 때문에 모집단의 수(N)으로 나누는 것이 옳다.
            // by smbg11
            //stddev = Math.Sqrt(devsum / (n - 1));
            stddev = Math.Sqrt(devsum / (n));

            return stddev;
        }

        // Sample standard deviation, 표준편차(S)
        public static double StdDev(double[] data)
        {
            double stddev, mean, devsum;
            int n = data.Count();

            if (n == 0)
                return 0;

            mean = ArithmeticMean(data);
            devsum = data.Sum(d => Math.Pow((double)d - mean, 2));
            // 표준편차 구할 때에 모집단이 샘플링 검사일 경우에는 모집단의 수(n)에서 1을 뺀 값으로 나누는 것이 맞다.
            // 하지만 우리는 샘플링검사가 아니라 전수검사 기준이기 때문에 모집단의 수(N)으로 나누는 것이 옳다.
            // by smbg11
            //stddev = Math.Sqrt(devsum / (n - 1));
            stddev = Math.Sqrt(devsum / (n));

            return stddev;
        }
    }
}
