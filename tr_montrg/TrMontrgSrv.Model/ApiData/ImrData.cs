using System;
using System.Text.Json.Serialization;

namespace CSG.MI.TrMontrgSrv.Model.ApiData
{
    public class ImrData
    {
        /// <summary>
        /// Data captured date and time
        /// </summary>
        [JsonPropertyName("dt")]
        public DateTime? Dt { get; set; }

        /// <summary>
        /// Max. temperature
        /// </summary>
        [JsonPropertyName("tmax")]
        public float? TMax { get; set; }

        /// <summary>
        /// Min. temperature
        /// </summary>
        [JsonPropertyName("tmin")]
        public float? TMin { get; set; }

        /// <summary>
        /// Min. temperature of the frame (ambient temperature)
        /// </summary>
        [JsonPropertyName("tmin_frame")]
        public float? TMinFrame { get; set; }

        /// <summary>
        /// Difference between max. temperature and min. temperature of the frame
        /// </summary>
        [JsonPropertyName("diff")]
        public float? Diff { get; set; }

        /// <summary>
        /// MR(Moving Range) value of max. temperature
        /// </summary>
        [JsonPropertyName("mr_max")]
        public float? MrMax { get; set; }

        /// <summary>
        /// Signed MR(Moving Range) value of max. temperature
        /// </summary>
        [JsonPropertyName("mr_max_sign")]
        public float? MrSignMax { get; set; }

        /// <summary>
        /// X-bar value of max. temperatures
        /// </summary>
        [JsonPropertyName("xbar_max")]
        public float? XBarMax { get; set; }

        /// <summary>
        /// MR-bar value of max. temperatures
        /// </summary>
        [JsonPropertyName("mr_bar_max")]
        public float? MrBarMax { get; set; }

        /// <summary>
        /// UCL of individual chart of max. temperature
        /// </summary>
        [JsonPropertyName("ucl_i_max")]
        public float? UclIMax { get; set; }

        /// <summary>
        /// LCL of individual chart of max. temperature
        /// </summary>
        [JsonPropertyName("lcl_i_max")]
        public float? LclIMax { get; set; }

        /// <summary>
        /// UCL of MR chart of max. temperature
        /// </summary>
        [JsonPropertyName("ucl_mr_max")]
        public float? UclMrMax { get; set; }

        /// <summary>
        /// LCL of MR chart of max. temperature
        /// </summary>
        [JsonPropertyName("lcl_mr_max")]
        public float? LclMrMax { get; set; }

        /// <summary>
        /// MR(Moving Range) value of diff. temperature
        /// </summary>
        [JsonPropertyName("mr_diff")]
        public float? MrDiff { get; set; }

        /// <summary>
        /// Signed MR(Moving Range) value of diff. temperature
        /// </summary>
        [JsonPropertyName("mr_diff_sign")]
        public float? MrSignDiff { get; set; }

        /// <summary>
        /// X-bar value of diff. temperatures
        /// </summary>
        [JsonPropertyName("xbar_diff")]
        public float? XBarDiff { get; set; }

        /// <summary>
        /// MR-bar value of diff. temperatures
        /// </summary>
        [JsonPropertyName("mr_bar_diff")]
        public float? MrBarDiff { get; set; }

        /// <summary>
        /// UCL of individual chart of diff. temperature
        /// </summary>
        [JsonPropertyName("ucl_i_diff")]
        public float? UclIDiff { get; set; }

        /// <summary>
        /// LCL of individual chart of diff. temperature
        /// </summary>
        [JsonPropertyName("lcl_i_diff")]
        public float? LclIDiff { get; set; }

        /// <summary>
        /// UCL of MR chart of diff. temperature
        /// </summary>
        [JsonPropertyName("ucl_mr_diff")]
        public float? UclMrDiff { get; set; }

        /// <summary>
        /// LCL of MR chart of diff. temperature
        /// </summary>
        [JsonPropertyName("lcl_mr_diff")]
        public float? LclMrDiff { get; set; }

    }
}
