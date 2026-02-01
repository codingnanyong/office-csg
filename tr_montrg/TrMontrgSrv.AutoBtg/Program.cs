using CSG.MI.TrMontrgSrv.AutoBtg;
using CSG.MI.TrMontrgSrv.AutoBtg.Generator;
using CSG.MI.TrMontrgSrv.AutoBtg.Inspector;
using CSG.MI.TrMontrgSrv.AutoBtg.Inspector.Interface;
using CSG.MI.TrMontrgSrv.AutoBtg.PingCore;
using CSG.MI.TrMontrgSrv.Model.Inspection;

var batchGenerator = new IoTBatchGenerator();
var pingWrapper = new PingWrapper();

var inspector = new IoTInspector(batchGenerator, pingWrapper);

await RunInspection(inspector);

static async Task RunInspection(IIoTInspector<InspcDevice> inspector)
{
    try
    {
        var devices = await inspector.GetDatas();
        inspector.InspectionData(devices);
    }
    catch (Exception ex)
    {
        StaticLogger.Logger.LogError($"An error occurred during inspection: {ex.Message}");
    }
}