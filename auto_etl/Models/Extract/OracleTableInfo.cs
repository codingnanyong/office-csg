//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

namespace CSG.MI.AutoETL.Models.Extract
{
    public class OracleTableInfo : ITableInfo
    {
        public string Db { get; set; }
        public string Name { get; set; }
    }
}
