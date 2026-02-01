//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Serialization;

namespace CSG.MI.AutoETL.Models.Extract
{
    public class QueryCondition
    {
        #region Properties

        [XmlArray]
        public List<Column> SelectClause { get; set; }

        public string FromClause { get; set; }

        public string WhereClause { get; set; }

        #endregion

        #region Public Methods

        public List<string> GetDataTypes()
        {
            return SelectClause.Select(x => x.DataType.ToString())
                               .ToList();
        }

        public List<string> GetColumnNames()
        {
            return SelectClause.Select(x => String.IsNullOrEmpty(x.As) ? x.Text : x.As).ToList();
        }

        #endregion
    }
}
