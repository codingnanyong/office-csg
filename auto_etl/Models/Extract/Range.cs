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
    /// <summary>
    /// 시작과 종료일자에 대한 유효성 검사와 구간 중복 검사를 통해
    /// 유효한 범위 생성
    /// </summary>
    public class Range
    {
        #region Fields

        private DateTime _start = DateTime.Today;

        private DateTime _end = DateTime.Today;

        #endregion

        #region Constructors

        public Range() { }
        public Range(DateTime start, DateTime end, TimeSpan margin)
        {
            MaxEnd = DateTime.Now - margin;// .AddDays(margin); // 수집 범위를 최대 당일 현재 시분초까지

            if(start > end)
                throw new ArgumentException("Start date must be earlier than End date");

            Start = start;
            End = end;
        }

        #endregion

        #region Properties

        public DateTime MaxEnd { get; set; }

        public DateTime Start
        {
            get { return _start; }
            set
            {
                //if (End < value)
                //{
                //    throw new ArgumentException("Start date must be earlier than End date");
                //}

                var s = MaxEnd;
                if (value >= s)
                {
                    _start = s;
                }
                else
                {
                    _start = value;
                }
            }

        }

        public DateTime End
        {
            get { return _end; }
            set
            {
                //if (Start > value)
                //{
                //    throw new ArgumentException("End date must be later than Start date");
                //}

                var e = MaxEnd;
                if (value >= e)
                {
                    _end = e;
                }
                else
                {
                    _end = value;
                }
            }
        }

        #endregion

        #region Public Methods

        public bool IsOverlapped(Range old)
        {
            //return range.Start <= End && Start <= range.End;
            return old.Start < End && Start < old.End;
        }

        public Range DeepCopy()
        {
            Range copy = new Range();
            copy.MaxEnd = MaxEnd;
            copy.Start = Start;
            copy.End = End;

            return copy;
        }

        #endregion
    }
}
