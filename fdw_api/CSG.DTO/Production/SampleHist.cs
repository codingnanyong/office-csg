using System;

namespace CSG.MI.DTO.Production
{
    //PCC 기준 이력 정보
    public class SampleHist
    {
        public string OpCd { get; set; } = String.Empty;

        public string OpName { get; set; } = String.Empty;

        public string OpLocalName { get; set; } = String.Empty;

        public string OpChk { get; set; } = String.Empty;

        public string PlanDate { get; set; } = String.Empty;

        public string ProdDate { get; set; } = String.Empty;

        public string ProdTime { get; set; } = String.Empty;

        public string Status { get; set; } = String.Empty;

    }
}
