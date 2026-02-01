//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Models;
using CSG.MI.AutoETL.Models.Extract;

namespace CSG.MI.AutoETL.Configs
{
    public interface IDbServerConfigBuilder
    {
        /// <summary>
        /// 데이터 추출을 위한 초기 설정파일 생성
        /// </summary>
        /// <param name="companyString"></param>
        /// <param name="dbConnInfo"></param>
        void Build(string companyString, DbConnInfo dbConnInfo);

        /// <summary>
        /// 설정파일 초기화
        /// </summary>
        /// <param name="configs"></param>
        void Update(ExtractorConfig config);
    }
}
