using System;

namespace CSG.MI.DTO.Production
{
    public class FactoryTotal
    {
        public string Year { get; set; } = String.Empty;

        public string Factory { get; set; } = String.Empty;

        public long TotalCnt { get; set; } = 0;

        public decimal TotalQty { get; set; } = Decimal.Zero;
    }
}
