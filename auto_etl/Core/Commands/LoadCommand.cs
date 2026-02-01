//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Core.Load;
using NLog;
using System.Diagnostics;

namespace CSG.MI.AutoETL.Core.Commands
{
    public class LoadCommand : ICommand
    {
        #region Fields

        private Logger _logger = NLog.LogManager.GetCurrentClassLogger();

        #endregion

        #region Properties
        #endregion

        #region ICommand Members

        public void Execute()
        {
            _logger.Info(@"[{0}][{1}]", "LoadCommand", "Begin");
            Stopwatch sw = new Stopwatch();
            sw.Start();

            IDbServerLoader importer = new PgServerLoader();
            importer.Load();

            sw.Stop();
            _logger.Info(@"[{0}][{1}][{2}]", "LoadCommand", "End", sw.Elapsed);
        }

        #endregion

        #region Public Methods

        #endregion
    }
}
