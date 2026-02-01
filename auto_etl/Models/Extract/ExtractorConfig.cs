//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Core;
using System;
using System.ComponentModel;
using System.Globalization;
using System.Xml;
using System.Xml.Serialization;

namespace CSG.MI.AutoETL.Models.Extract
{
    public class ExtractorConfig
    {
        #region Properties

        public int Priority { get; set; }

        [XmlElement("CompanyCode")]
        public string CompanyString { get; set; }

        [XmlIgnore]
        public CompanyCode Company
        {
            get
            {
                return (CompanyCode)Enum.Parse(typeof(CompanyCode), CompanyString?.ToUpper());
            }
            set
            {
                CompanyString = value.ToString();
            }
        }

        public string Category { get; set; }

        public DbConnInfo DbConn { get; set; }

        public QueryCondition QueryCondition { get; set; }
        /// <summary>
        /// InitCommand 시점에 초기화
        /// </summary>
        public string DeltaField { get; set; } = "None";
        public string DeltaFieldType { get; set; } = "None";

        /// <summary>
        /// 최초 데이터 내보내기 시작 시점
        /// 데이터 내보내기 이력이 없는 경우 사용됨
        /// 지역마다 표준시간대
        /// </summary>
        public DateTime InitialStart { get; set; }
        public String InitialStartString { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }

        /// <summary>
        /// 윈도우 크기
        /// </summary>
        private TimeSpan _deltaValue;

        [XmlIgnore]
        public TimeSpan DeltaValue
        {
            get { return _deltaValue; }
            set { _deltaValue = value; }
        }

        [Browsable(false)]
        [XmlElement(DataType = "duration", ElementName = "DeltaValue")]
        public string DeltaString
        {
            get
            {
                return DeltaValue.ToString(@"dd\:hh\:mm\:ss");
            }
            set
            {
                DeltaValue = string.IsNullOrEmpty(value) ?  
                    TimeSpan.Zero : TimeSpan.ParseExact(value, @"dd\:hh\:mm\:ss", CultureInfo.InvariantCulture);
            }
        }

        /// <summary>
        /// 최근 데이터는 갱신될 수 있으므로 수집 가능한 최대 범위(Now-Margin)
        /// </summary>

        private TimeSpan _margin;

        [XmlIgnore]
        public TimeSpan Margin
        {
            get { return _margin; }
            set { _margin = value; }
        }

        [Browsable(false)]
        [XmlElement(DataType = "duration", ElementName = "Margin")]
        public string MarginString
        {
            get
            {
                return Margin.ToString(@"dd\:hh\:mm\:ss");
            }
            set
            {
                Margin = string.IsNullOrEmpty(value) ?
                    TimeSpan.Zero : TimeSpan.ParseExact(value, @"dd\:hh\:mm\:ss", CultureInfo.InvariantCulture);
            }
        }

        /// <summary>
        /// 적재 테이블
        /// </summary>
        public string LoadTable { get; set; }

        [XmlIgnore]
        public bool IsSuccessfullyCompleted { get; set; }

        #endregion

        #region Public Methods

        /// <summary>
        /// FDW 적재 정보로 사용됨
        /// </summary>
        /// <returns></returns>
        public string GetFileName()
        {
            var ip = DbConn.Server.Split(new[] { "\\" }, StringSplitOptions.None)[0];
            return $"{CompanyString.ToLower()}-{DbConn.Id}-{ip.ToLower()}-{DbConn.Database.ToLower()}-{LoadTable.ToLower()}";
        }

        #endregion
    }
}
