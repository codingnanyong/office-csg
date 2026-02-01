using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("worksheets_rate", Schema = "services")]
    public class WsWipRateEntity
    {
        [Column("op_cd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;

        [Column("op_name", TypeName = "varchar(500)")]
        public string OpName { get; set; } = String.Empty;

        [Column("op_local_name", TypeName = "varchar(500)")]
        public string OpLocalName { get; set; } = String.Empty;

        [Column("op_leadtime", TypeName = "numeric")]
        public decimal OpLeadTime { get; set; } = Decimal.Zero;

        [Column("op_capacity", TypeName = "numeric")]
        public decimal OpCatacity {  get; set; } = Decimal.Zero;

        // Status Codes [I:입고, T:투입, O:완료, K:보관, S:중단, H:대기, A:도착, P:발행, D:삭제]

        [Column("i_cnt",TypeName ="bigint")]
        public long iCnt { get; set; } = 0;

        [Column("i_qty", TypeName = "numeric")]
        public decimal iQty { get; set; } = Decimal.Zero;

        [Column("t_cnt", TypeName = "bigint")]
        public long tCnt { get; set; } = 0;

        [Column("t_qty", TypeName = "numeric")]
        public decimal tQty { get; set; } = Decimal.Zero;

        [Column("o_cnt", TypeName = "bigint")]
        public long oCnt { get; set; } = 0;

        [Column("o_qty", TypeName = "numeric")]
        public decimal oQty { get; set; } = Decimal.Zero;

        [Column("k_cnt", TypeName = "bigint")]
        public long kCnt { get; set; } = 0;

        [Column("k_qty", TypeName = "numeric")]
        public decimal kQty { get; set; } = Decimal.Zero;

        [Column("s_cnt", TypeName = "bigint")]
        public long sCnt { get; set; } = 0;

        [Column("s_qty", TypeName = "numeric")]
        public decimal sQty { get; set; } = Decimal.Zero;

        [Column("h_cnt", TypeName = "bigint")]
        public long hCnt { get; set; } = 0;

        [Column("h_qty", TypeName = "numeric")]
        public decimal hQty { get; set; } = Decimal.Zero;

        [Column("a_cnt", TypeName = "bigint")]
        public long aCnt { get; set; } = 0;

        [Column("a_qty", TypeName = "numeric")]
        public decimal aQty { get; set; } = Decimal.Zero;

        [Column("p_cnt", TypeName = "bigint")]
        public long pCnt { get; set; } = 0;

        [Column("p_qty", TypeName = "numeric")]
        public decimal pQty { get; set; } = Decimal.Zero;

        [Column("d_cnt", TypeName = "bigint")]
        public long dCnt { get; set; } = 0;

        [Column("d_qty", TypeName = "numeric")]
        public decimal dQty { get; set; } = Decimal.Zero;
    }
}
