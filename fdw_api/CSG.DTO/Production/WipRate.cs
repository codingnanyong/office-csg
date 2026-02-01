using System;

namespace CSG.MI.DTO.Production
{
    public class WipRate
    {
        public string OpCd { get; set; } = String.Empty;

        public string OpName { get; set; } = String.Empty;

        public string OpLocalName { get; set; } = String.Empty;

        public decimal OpLeadTime { get; set; } = Decimal.Zero;

        public decimal OpCatacity { get; set; } = Decimal.Zero;

        // Status Codes [I:입고, T:투입, O:완료, K:보관, S:중단, H:대기, A:도착, P:발행, D:삭제]

        public long iCnt { get; set; } = 0;

        public decimal iQty { get; set; } = Decimal.Zero;

        public long tCnt { get; set; } = 0;

        public decimal tQty { get; set; } = Decimal.Zero;

        public long oCnt { get; set; } = 0;

        public decimal oQty { get; set; } = Decimal.Zero;

        public long kCnt { get; set; } = 0;

        public decimal kQty { get; set; } = Decimal.Zero;

        public long sCnt { get; set; } = 0;

        public decimal sQty { get; set; } = Decimal.Zero;

        public long hCnt { get; set; } = 0;

        public decimal hQty { get; set; } = Decimal.Zero;

        public long aCnt { get; set; } = 0;

        public decimal aQty { get; set; } = Decimal.Zero;

        public long pCnt { get; set; } = 0;

        public decimal pQty { get; set; } = Decimal.Zero;

        public long dCnt { get; set; } = 0;

        public decimal dQty { get; set; } = Decimal.Zero;
    }
}
