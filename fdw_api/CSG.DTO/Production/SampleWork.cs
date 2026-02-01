using System;

namespace CSG.MI.DTO.Production
{
    public class SampleWork
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public string OpCd { get; set; } = String.Empty;

        public string OpName { get; set; } = String.Empty;

        public string OpLocalName { get; set; } = String.Empty;

        public string Pm { get; set; } = String.Empty;

        public string PmName { get; set; } = String.Empty;

        public string Model { get; set; } = String.Empty;

        public string Season { get; set; } = String.Empty;

        public string BomId { get; set; } = String.Empty;

        public string StyleCd { get; set; } = String.Empty;

        public string DevColorwayId { get; set; } = String.Empty;

        public string PlanDate { get; set; } = String.Empty;

        public string ProdDate { get; set; } = String.Empty;

        public string ProdTime { get; set; } = String.Empty;

        public string Status { get; set; } = String.Empty;

        public decimal ProdQty { get; set; } = Decimal.Zero;
    }
}
