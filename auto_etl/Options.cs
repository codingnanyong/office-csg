//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using CommandLine;

namespace CSG.MI.AutoETL
{
    /// <summary>
    /// -r "config" -p "C:\MI\AutoETL\src\AutoETL\bin\Debug\_cfg"
    /// -r "extract" -p "C:\MI\AutoETL\src\AutoETL\bin\Debug\_cfg"
    /// -r "load" -p "C:\MI\AutoETL\src\AutoETL\bin\Debug\_cfg"
    /// </summary>
    public class Options
    {
        [Option('r', "run", Required = true, HelpText = "-r \"[config|update|extract|load]\"")]
        public string Run { get; set; }
        [Option('p', "path", Required = true, HelpText = "-p \"config directory path\"")]
        public string ConfigDirPath { get; set; }
    }
}
