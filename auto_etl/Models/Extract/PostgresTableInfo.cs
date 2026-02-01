//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: teahyeon.ryu
// 개발일: 2023.04.07
// 수정내역:
// - 2023.04.07 taehyeon.ryu : Initial Version
//===============================================================================

namespace CSG.MI.AutoETL.Models.Extract
{
	public class PostgresTableInfo : ITableInfo
	{
		public string Db { get; set; }
		public string Schema { get; set; }
		public string Name { get; set; }
	}
}
