using System;

namespace CSG.MI.DTO.Production
{
    public class DailyStatus
    {
        public string Factory { get; set; } = String.Empty;

        public string OpCd { get; set; } = String.Empty;

        public string OpName { get; set; } = String.Empty;

        public string OpLocalName { get; set; } = String.Empty;

        public decimal? PlanCnt { get; set; } = Decimal.Zero;

        public decimal? PlanQty { get; set; } = Decimal.Zero;

        public decimal? ProdCnt { get; set; } = Decimal.Zero;

        public decimal? ProdQty { get; set; } = Decimal.Zero;

        public decimal? Rate { get; set; } = Decimal.Zero;
    }
}