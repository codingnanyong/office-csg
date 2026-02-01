using System;

namespace CSG.MI.DTO.Production
{
    public class WipBottleneck
    {
        public string Factory { get; set; } = String.Empty;

        public string OpCd { get; set; } = String.Empty;

        public string OpName { get; set; } = String.Empty;

        public string OpLocalName { get; set; } = String.Empty;

        public decimal OpCapacity {  get; set; } = Decimal.Zero;

        public decimal OpQty { get; set; } = Decimal.Zero;
    }
}
