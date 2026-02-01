using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.Json.Converters;

namespace CSG.MI.TrMontrgSrv.Model.Json
{
    public class ConfigJson
    {
        #region Properties

        [JsonPropertyName("APP_NAME")]
        public string AppName { get; set; }

        [JsonPropertyName("APP_VERSION")]
        public string AppVersion { get; set; }

        [JsonPropertyName("DEVICE_ID")]
        public string DeviceId { get; set; }

        [JsonPropertyName("PLANT_ID")]
        public string PlantId { get; set; }

        [JsonPropertyName("LOCATION_ID")]
        public string LocationId { get; set; }

        [JsonPropertyName("TEST_MODE")]
        public bool TestMode { get; set; }

        [JsonPropertyName("HZ_CAP")]
        public float HzCap { get; set; }

        [JsonPropertyName("RGB_CAPTURE_W")]
        public int RgbCaptureW { get; set; }

        [JsonPropertyName("RGB_CAPTURE_H")]
        public int RgbCaptureH { get; set; }

        [JsonPropertyName("ENABLE_RGB_THREAD")]
        public bool EnableRgbThread { get; set; }

        [JsonPropertyName("ENABLE_IR_THREAD")]
        public bool EnableIrThread { get; set; }

        [JsonPropertyName("ENABLE_FLASK_THREAD")]
        public bool EnableFlaskThread { get; set; }

        [JsonPropertyName("ENABLE_FTP_UPLOAD")]
        public bool EnableFtpUpload { get; set; }

        [JsonPropertyName("ENABLE_ROI")]
        public bool EnableRoi { get; set; }

        [JsonPropertyName("ENABLE_FACE_DETECTION")]
        public bool EnableFaceDetection { get; set; }

        [JsonPropertyName("SHOW_DISPLAY")]
        public bool ShowDisplay { get; set; }

        [JsonPropertyName("OVERLAY_BOX_TEMP_ON_RGB")]
        public bool OverlayBoxTempOnRgb { get; set; }

        [JsonPropertyName("OVERLAY_ROI_TEMP_ON_RGB")]
        public bool OverlayRoiTempOnRgb { get; set; }

        [JsonPropertyName("OVERLAY_FACE_BOX_ON_RGB")]
        public bool OverlayFaceBoxOnRgb { get; set; }

        [JsonPropertyName("OVERLAY_BOX_MAX_TEMP_ONLY")]
        public bool OverlayBoxMaxTempOnly { get; set; }

        [JsonPropertyName("RGB_OVERLAY_ADJ_SCALE")]
        public float RgbOverlayAdjScale { get; set; }

        [JsonPropertyName("RGB_OVERLAY_ADJ_X")]
        public int RgbOverlayAdjX { get; set; }

        [JsonPropertyName("RGB_OVERLAY_ADJ_Y")]
        public int RgbOverlayAdjY { get; set; }

        [JsonPropertyName("SAVE_FRAMES")]
        public bool SaveFrames { get; set; }

        [JsonPropertyName("SAVE_TEMP_DATA")]
        public bool SaveTempData { get; set; }

        [JsonPropertyName("THRESHOLD_TEMPERATURE")]
        public int ThresholdTemperature { get; set; }

        [JsonPropertyName("THRESHOLD_TEMPERATURE_OF_ROI")]
        public int ThresholdTemperatureOfRoi { get; set; }

        [JsonPropertyName("WARN_TEMPERATURE")]
        public int WarnTemperature { get; set; }

        [JsonPropertyName("CMAP_THR_MIN")]
        public int CMapThresholdMin { get; set; }

        [JsonPropertyName("CMAP_THR_MAX")]
        public int CMapThresholdMax { get; set; }

        [JsonPropertyName("DIFF_GOOD_TEMP")]
        public int[] DiffGoodTemperature { get; set; } = new int[2];

        [JsonPropertyName("DIFF_OK_TEMP")]
        public int[] DiffOkTemperature { get; set; } = new int[2];

        [JsonPropertyName("DIFF_ATTENTION_TEMP")]
        public int[] DiffAttentionTemperature { get; set; } = new int[2];

        [JsonPropertyName("DIFF_DANGEROUS_TEMP")]
        public int[] DiffDangerousTemperature { get; set; } = new int[2];

        [JsonPropertyName("FRAME_TEMP_COLOR")]
        public int[] FrameTempColor { get; set; } = new int[3];

        [JsonPropertyName("BOX_TEMP_COLOR")]
        public int[] BoxTempColor { get; set; } = new int[3];

        [JsonPropertyName("ROI_TEMP_COLOR")]
        public int[] RoiTempColor { get; set; } = new int[3];

        [JsonPropertyName("FACE_BOX_COLOR")]
        public int[] FaceBoxColor { get; set; } = new int[3];

        [JsonPropertyName("WARN_TEMP_COLOR")]
        public int[] WarnTempColor { get; set; } = new int[3];

        [JsonPropertyName("DIFF_GOOD_TEMP_COLOR")]
        public int[] DiffGoodTempColor { get; set; } = new int[3];

        [JsonPropertyName("DIFF_OK_TEMP_COLOR")]
        public int[] DiffOkTempColor { get; set; } = new int[3];

        [JsonPropertyName("DIFF_ATTENTION_TEMP_COLOR")]
        public int[] DiffAttentionTempColor { get; set; } = new int[3];

        [JsonPropertyName("DIFF_DANGEROUS_TEMP_COLOR")]
        public int[] DiffDangerousTempColor { get; set; } = new int[3];

        [JsonPropertyName("LOG_DIR")]
        public string LogDir { get; set; }

        [JsonPropertyName("LOG_DIRS")]
        //[JsonConverter(typeof(DictionaryStringObjectJsonConverter))]
        public Dictionary<string, string> LogDirs { get; set; } = new Dictionary<string, string>();

        [JsonPropertyName("WIN_SIZE")]
        public int[] WinSize { get; set; } = new int[2];

        [JsonPropertyName("RGB_WIN_NAME")]
        public string RgbWinName { get; set; }

        [JsonPropertyName("IR_WIN_NAME")]
        public string IrWinName { get; set; }

        [JsonPropertyName("X_DISPLAY_ADDR")]
        public string XDisplayAddr { get; set; }

        [JsonPropertyName("DATA_SAVING_INTERVAL")]
        public int DataSavingInterval { get; set; }

        [JsonPropertyName("FLASK_HOST")]
        public string FlaskHost { get; set; }

        [JsonPropertyName("FLASK_PORT")]
        public int FlaskPort { get; set; }

        [JsonPropertyName("FTP_HOST")]
        public string FtpHost { get; set; }

        [JsonPropertyName("FTP_PORT")]
        public int FtpPort { get; set; }

        [JsonPropertyName("FTP_ID")]
        public string FtpId { get; set; }

        [JsonPropertyName("FTP_PW")]
        public string FtpPw { get; set; }

        [JsonPropertyName("MAX_EXISTING_FILE_UPLOAD")]
        public int MaxExistingFileUpload { get; set; }

        [JsonPropertyName("ROI_COORD")]
        //[JsonConverter(typeof(DictionaryStringObjectJsonConverter))]
        public Dictionary<string, int[]> RoiCoord { get; set; } = new Dictionary<string, int[]>();

        #endregion

        #region JsonIgnore

        [JsonIgnore]
        public string Ymd { get; set; }

        [JsonIgnore]
        public string Hms { get; set; }

        [JsonIgnore]
        public string JsonString { get; set; }

        #endregion

        public override bool Equals(Object obj)
        {
            if ((obj == null) || !this.GetType().Equals(obj.GetType()))
            {
                return false;
            }
            else
            {
                ConfigJson cfg = (ConfigJson)obj;

                if (AppName == cfg.AppName &&
                    AppVersion == cfg.AppVersion &&
                    DeviceId == cfg.DeviceId &&
                    PlantId == cfg.PlantId &&
                    LocationId == cfg.LocationId &&
                    TestMode == cfg.TestMode &&
                    HzCap == cfg.HzCap &&
                    RgbCaptureW == cfg.RgbCaptureW &&
                    RgbCaptureH == cfg.RgbCaptureH &&
                    EnableRgbThread == cfg.EnableRgbThread &&
                    EnableIrThread == cfg.EnableIrThread &&
                    EnableFlaskThread == cfg.EnableFlaskThread &&
                    EnableFtpUpload == cfg.EnableFtpUpload &&
                    EnableRoi == cfg.EnableRoi &&
                    EnableFaceDetection == cfg.EnableFaceDetection &&
                    ShowDisplay == cfg.ShowDisplay &&
                    OverlayBoxTempOnRgb == cfg.OverlayBoxTempOnRgb &&
                    OverlayRoiTempOnRgb == cfg.OverlayRoiTempOnRgb &&
                    OverlayFaceBoxOnRgb == cfg.OverlayFaceBoxOnRgb &&
                    OverlayBoxMaxTempOnly == cfg.OverlayBoxMaxTempOnly &&
                    RgbOverlayAdjScale == cfg.RgbOverlayAdjScale &&
                    RgbOverlayAdjX == cfg.RgbOverlayAdjX &&
                    RgbOverlayAdjY == cfg.RgbOverlayAdjY &&
                    SaveFrames == cfg.SaveFrames &&
                    SaveTempData == cfg.SaveTempData &&
                    ThresholdTemperature == cfg.ThresholdTemperature &&
                    ThresholdTemperatureOfRoi == cfg.ThresholdTemperatureOfRoi &&
                    WarnTemperature == cfg.WarnTemperature &&
                    CMapThresholdMin == cfg.CMapThresholdMin &&
                    CMapThresholdMax == cfg.CMapThresholdMax &&
                    DiffGoodTemperature[0] == cfg.DiffGoodTemperature[0] &&
                    DiffGoodTemperature[1] == cfg.DiffGoodTemperature[1] &&
                    DiffOkTemperature[0] == cfg.DiffOkTemperature[0] &&
                    DiffOkTemperature[1] == cfg.DiffOkTemperature[1] &&
                    DiffAttentionTemperature[0] == cfg.DiffAttentionTemperature[0] &&
                    DiffAttentionTemperature[1] == cfg.DiffAttentionTemperature[1] &&
                    DiffDangerousTemperature[0] == cfg.DiffDangerousTemperature[0] &&
                    DiffDangerousTemperature[1] == cfg.DiffDangerousTemperature[1] &&
                    FrameTempColor[0] == cfg.FrameTempColor[0] &&
                    FrameTempColor[1] == cfg.FrameTempColor[1] &&
                    FrameTempColor[2] == cfg.FrameTempColor[2] &&
                    BoxTempColor[0] == cfg.BoxTempColor[0] &&
                    BoxTempColor[1] == cfg.BoxTempColor[1] &&
                    BoxTempColor[2] == cfg.BoxTempColor[2] &&
                    RoiTempColor[0] == cfg.RoiTempColor[0] &&
                    RoiTempColor[1] == cfg.RoiTempColor[1] &&
                    RoiTempColor[2] == cfg.RoiTempColor[2] &&
                    FaceBoxColor[0] == cfg.FaceBoxColor[0] &&
                    FaceBoxColor[1] == cfg.FaceBoxColor[1] &&
                    FaceBoxColor[2] == cfg.FaceBoxColor[2] &&
                    WarnTempColor[0] == cfg.WarnTempColor[0] &&
                    WarnTempColor[1] == cfg.WarnTempColor[1] &&
                    WarnTempColor[2] == cfg.WarnTempColor[2] &&
                    DiffGoodTempColor[0] == cfg.DiffGoodTempColor[0] &&
                    DiffGoodTempColor[1] == cfg.DiffGoodTempColor[1] &&
                    DiffGoodTempColor[2] == cfg.DiffGoodTempColor[2] &&
                    DiffOkTempColor[0] == cfg.DiffOkTempColor[0] &&
                    DiffOkTempColor[1] == cfg.DiffOkTempColor[1] &&
                    DiffOkTempColor[2] == cfg.DiffOkTempColor[2] &&
                    DiffAttentionTempColor[0] == cfg.DiffAttentionTempColor[0] &&
                    DiffAttentionTempColor[1] == cfg.DiffAttentionTempColor[1] &&
                    DiffAttentionTempColor[2] == cfg.DiffAttentionTempColor[2] &&
                    DiffDangerousTempColor[0] == cfg.DiffDangerousTempColor[0] &&
                    DiffDangerousTempColor[1] == cfg.DiffDangerousTempColor[1] &&
                    DiffDangerousTempColor[2] == cfg.DiffDangerousTempColor[2] &&
                    LogDir == cfg.LogDir &&
                    LogDirs.Count == cfg.LogDirs.Count &&
                    WinSize[0] == cfg.WinSize[0] &&
                    WinSize[1] == cfg.WinSize[1] &&
                    RgbWinName == cfg.RgbWinName &&
                    IrWinName == cfg.IrWinName &&
                    XDisplayAddr == cfg.XDisplayAddr &&
                    DataSavingInterval == cfg.DataSavingInterval &&
                    FlaskHost == cfg.FlaskHost &&
                    FlaskPort == cfg.FlaskPort &&
                    FtpHost == cfg.FtpHost &&
                    FtpPort == cfg.FtpPort &&
                    FtpId == cfg.FtpId &&
                    FtpPw == cfg.FtpPw &&
                    MaxExistingFileUpload == cfg.MaxExistingFileUpload)
                {
                    if (RoiCoord.Count != cfg.RoiCoord.Count)
                        return false;

                    var c1RoiCoord = RoiCoord.Values.ToArray();
                    var c2RoiCoord = cfg.RoiCoord.Values.ToArray();
                    for (int i = 0; i < c1RoiCoord.Length; i++)
                    {
                        int[] value = c1RoiCoord[i];
                        for (int j = 0; j < value.Length; j++)
                        {
                            if (c2RoiCoord[i][j] != value[j])
                                return false;
                        }
                    }

                    return true;
                }

                return false;
            }
        }

        public override int GetHashCode()
        {
            int hcode = AppName.GetHashCode() +
                        AppVersion.GetHashCode() +
                        DeviceId.GetHashCode() +
                        PlantId.GetHashCode() +
                        LocationId.GetHashCode() +
                        TestMode.GetHashCode() +
                        HzCap.GetHashCode() +
                        RgbCaptureW ^
                        RgbCaptureH +
                        EnableRgbThread.GetHashCode() +
                        EnableIrThread.GetHashCode() +
                        EnableFlaskThread.GetHashCode() +
                        EnableFtpUpload.GetHashCode() +
                        EnableRoi.GetHashCode() +
                        EnableFaceDetection.GetHashCode() +
                        ShowDisplay.GetHashCode() +
                        OverlayBoxTempOnRgb.GetHashCode() +
                        OverlayRoiTempOnRgb.GetHashCode() +
                        OverlayFaceBoxOnRgb.GetHashCode() +
                        OverlayBoxMaxTempOnly.GetHashCode() +
                        RgbOverlayAdjScale.GetHashCode() +
                        RgbOverlayAdjX ^
                        RgbOverlayAdjY +
                        SaveFrames.GetHashCode() +
                        SaveTempData.GetHashCode() +
                        ThresholdTemperature ^
                        ThresholdTemperatureOfRoi ^
                        WarnTemperature ^
                        CMapThresholdMin ^
                        CMapThresholdMax +
                        DiffGoodTemperature[0] ^ DiffGoodTemperature[1] ^
                        DiffOkTemperature[0] ^ DiffOkTemperature[1] ^
                        DiffAttentionTemperature[0] ^ DiffAttentionTemperature[1] ^
                        DiffDangerousTemperature[0] ^ DiffDangerousTemperature[1] ^
                        FrameTempColor[0] ^ FrameTempColor[1] ^ FrameTempColor[2] ^
                        BoxTempColor[0] ^ BoxTempColor[1] ^ BoxTempColor[2] ^
                        RoiTempColor[0] ^ RoiTempColor[1] ^ RoiTempColor[2] ^
                        FaceBoxColor[0] ^ FaceBoxColor[1] ^ FaceBoxColor[2] ^
                        WarnTempColor[0] ^ WarnTempColor[1] ^ WarnTempColor[2] ^
                        DiffGoodTempColor[0] ^ DiffGoodTempColor[1] ^ DiffGoodTempColor[2] ^
                        DiffOkTempColor[0] ^ DiffOkTempColor[1] ^ DiffOkTempColor[2] ^
                        DiffAttentionTempColor[0] ^ DiffAttentionTempColor[1] ^ DiffAttentionTempColor[2] ^
                        DiffDangerousTempColor[0] ^ DiffDangerousTempColor[1] ^ DiffDangerousTempColor[2] +
                        LogDir.GetHashCode() +
                        LogDirs.GetHashCode() +
                        WinSize[0] ^ WinSize[1] +
                        RgbWinName.GetHashCode() +
                        XDisplayAddr.GetHashCode() +
                        FlaskHost.GetHashCode() +
                        FlaskPort +
                        FtpHost.GetHashCode() +
                        FtpPort +
                        FtpId.GetHashCode() +
                        FtpPw.GetHashCode() +
                        MaxExistingFileUpload;
            RoiCoord.Values.ToList().ForEach(x => hcode += x[0] ^ x[1] ^ x[2]);

            return hcode.GetHashCode();
        }

        public Cfg ToCfg()
        {
            var cfg = new Cfg
            {
                //Id = 0,
                DeviceId = DeviceId,
                Ymd = Ymd,
                Hms = Hms,
                CaptureDt = (Ymd + Hms).ToDateTime().Value,
                CfgJson = this,
                //UpdatedDt = DateTime.Now
            };

            return cfg;
        }

    }
}
