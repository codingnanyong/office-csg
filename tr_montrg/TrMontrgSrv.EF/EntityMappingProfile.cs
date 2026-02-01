using AutoMapper;
using CSG.MI.TrMontrgSrv.EF.Core.Extentions;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using CSG.MI.TrMontrgSrv.Model.Dashboard;

namespace CSG.MI.TrMontrgSrv.EF
{
    public class EntityMappingProfile : Profile
    {
        public EntityMappingProfile()
        {
            CreateMaps();
        }

        private void CreateMaps()
        {
            //CreateMap<DeviceEntity, Device>().ForMember(x => x.Frames, opt => opt.Ignore());
            CreateMap<DeviceEntity, Device>().IgnoreMember(x => x.Frames)
                                             .IgnoreMember(x => x.Rois)
                                             .IgnoreMember(x => x.Boxes)
                                             .IgnoreMember(x => x.Cfgs)
                                             .IgnoreMember(x => x.Media)
                                             .IgnoreMember(x => x.DeviceCtrl)
                                             .IgnoreMember(x => x.FrameCtrl)
                                             .IgnoreMember(x => x.RoiCtrls)
                                             .IgnoreMember(x => x.BoxCtrl)
                                             .IgnoreMember(x => x.EvntLogs);
            //CreateMap<FrameEntity, Frame>().ForMember(x => x.Device, opt => opt.Ignore());
            CreateMap<FrameEntity, Frame>().IgnoreMember(x => x.Device);
            CreateMap<RoiEntity, Roi>().IgnoreMember(x => x.Device);
            CreateMap<BoxEntity, Box>().IgnoreMember(x => x.Device);
            CreateMap<CfgEntity, Cfg>().IgnoreMember(x => x.Device);
            CreateMap<MediumEntity, Medium>().IgnoreMember(x => x.Device);

            CreateMap<DeviceCtrlEntity, DeviceCtrl>().IgnoreMember(x => x.Device);
            CreateMap<FrameCtrlEntity, FrameCtrl>().IgnoreMember(x => x.Device);
            CreateMap<RoiCtrlEntity, RoiCtrl>().IgnoreMember(x => x.Device);
            CreateMap<BoxCtrlEntity, BoxCtrl>().IgnoreMember(x => x.Device);
            CreateMap<EvntLogEntity, EvntLog>().IgnoreMember(x => x.Device);
            CreateMap<GrpKeyEntity, GrpKey>();
            CreateMap<MailingAddrEntity, MailingAddr>();

            #region Temperature Dashboard 

            CreateMap<CurDeviceEntity, CurDevice>();
            /*
            CreateMap<CurDeviceEntity, CurDevice>()
                .ForMember(dest => dest.Frame, opt => opt.MapFrom(src => src.Frame))
                .ForMember(dest => dest.Boxes, opt => opt.MapFrom(src => src.Boxes))
                .ForMember(dest => dest.Roies, opt => opt.MapFrom(src => src.Roies));
            */
            CreateMap<CurFrameEntity, CurFrame>();
            CreateMap<CurBoxEntity, CurBox>();
            CreateMap<CurRoiEntity, CurRoi>();

            #endregion

            #region Inspection Device

            CreateMap<DeviceEntity, InspecDevice>();

            #endregion
        }
    }
}
