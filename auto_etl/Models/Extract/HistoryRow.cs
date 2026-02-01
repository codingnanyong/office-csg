//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using System;

namespace CSG.MI.AutoETL.Models.Extract
{
    public class HistoryRow
    {
        #region Fields

        private ExtractorConfig _cfg;

        #endregion

        #region Constructors

        public HistoryRow() { }

        public HistoryRow(ExtractorConfig cfg)
        {
            _cfg = cfg;
            FileName = _cfg.GetFileName();

            Range = new Range(_cfg.InitialStart, _cfg.InitialStart + cfg.DeltaValue, _cfg.Margin);
        }

        #endregion

        #region Properties

        public string FileName { get; set; }

        public Range Range { get; set; }

        public int RowCnt { get; set; }

        public TimeSpan EllipsedTime { get; set; }

        public DateTime WriteTime { get; set; }

        #endregion

        #region Public Methods

        public bool UpdateRange(TimeSpan delta, TimeSpan margin)
        {
            var OldRange = Range.DeepCopy();

            var newStart = OldRange.End;
            var newEnd = newStart + delta;

            Range = new Range(newStart, newEnd, margin);

            if (Range.IsOverlapped(OldRange))
            {
                // 복구
                Range = OldRange;
                return false;
            }
            else
            {
                return true;
            }
        }

        public override string ToString()
        {
            return $"{FileName}({Range.Start.ToString("yyyy-MM-dd HH:mm:ss.fffffff")} ~ {Range.End.ToString("yyyy-MM-dd HH:mm:ss.fffffff")})";
        }

        #endregion

    }
}
