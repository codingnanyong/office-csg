using AutoMapper;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using CSG.MI.TrMontrgSrv.Model.Dashboard;

namespace CSG.MI.TrMontrgSrv.EF
{
    public class ModelMappingProfile : Profile
    {
        public ModelMappingProfile()
        {
            CreateMaps();
        }

        private void CreateMaps()
        {
            CreateMap<DeviceEntity, Device>().ReverseMap();
            CreateMap<FrameEntity, Frame>().ReverseMap();
            CreateMap<RoiEntity, Roi>().ReverseMap();
            CreateMap<BoxEntity, Box>().ReverseMap();
            CreateMap<CfgEntity, Cfg>().ReverseMap();
            CreateMap<MediumEntity, Medium>().ReverseMap();

            CreateMap<DeviceCtrlEntity, DeviceCtrl>().ReverseMap();
            CreateMap<FrameCtrlEntity, FrameCtrl>().ReverseMap();
            CreateMap<RoiCtrlEntity, RoiCtrl>().ReverseMap();
            CreateMap<BoxCtrlEntity, BoxCtrl>().ReverseMap();
            CreateMap<EvntLogEntity, EvntLog>().ReverseMap();
            CreateMap<GrpKeyEntity, GrpKey>().ReverseMap();
            CreateMap<MailingAddrEntity, MailingAddr>().ReverseMap();

            #region Temperature Dashboard 

            CreateMap<CurDeviceEntity, CurDevice>().ReverseMap();
            CreateMap<CurFrameEntity, CurFrame>().ReverseMap();
            CreateMap<CurBoxEntity, CurBox>().ReverseMap();
            CreateMap<CurRoiEntity, CurRoi>().ReverseMap();

            #endregion

            #region Inspection Device

            CreateMap<DeviceEntity, InspecDevice>().ReverseMap();

            #endregion
        }
    }
}
