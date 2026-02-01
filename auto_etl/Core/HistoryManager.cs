//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Models.Extract;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;

namespace CSG.MI.AutoETL.Core
{
    class HistoryManager
    {
        #region Singleton

        private HistoryManager() { }

        private static readonly Lazy<HistoryManager> _instance = new Lazy<HistoryManager>(() => new HistoryManager());

        public static HistoryManager Instance { get { return _instance.Value; } }

        #endregion

        #region Public Methods

        public List<HistoryRow> GetHistory(string path)
        {
            if (File.Exists(path) == false)
            {
                SetHistory(path, new List<HistoryRow>());
            }

            using (var stream = File.OpenRead(path))
            {
                var serializer = new XmlSerializer(typeof(List<HistoryRow>));
                return serializer.Deserialize(stream) as List<HistoryRow>;
            }
        }

        public void SetHistory(string path, List<HistoryRow> history)
        {
            using (var writer = new StreamWriter(path))
            {
                var serializer = new XmlSerializer(history.GetType());
                serializer.Serialize(writer, history);
                writer.Flush();
            }
        }

        #endregion

    }
}
