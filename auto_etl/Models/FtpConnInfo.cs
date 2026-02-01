//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

namespace CSG.MI.AutoETL.Models
{
    public class FtpConnInfo
    {
        public string Server { get; set; }

        public int Port { get; set; }

        public string UserId { get; set; }

        public string Password { get; set; }
    }
}