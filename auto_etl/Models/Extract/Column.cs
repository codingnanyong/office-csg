//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using System.Data;
using System.Xml.Serialization;

namespace CSG.MI.AutoETL.Models.Extract
{
    [XmlRoot]
    public class Column
    {
        [XmlAttribute]
        public string Text { get; set; }

        [XmlAttribute]
        public DbType DataType { get; set; }

        [XmlAttribute]
        public string As { get; set; }
    }
}
