using AutoMapper;
using CSG.MI.DAO.Feedback;
using CSG.MI.DAO.Production.PCC;
using CSG.MI.DAO.Production.RTLS;
using CSG.MI.DTO.Analysis;
using CSG.MI.DTO.Department;
using CSG.MI.DTO.Feedback;
using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Entities.Department;
using CSG.MI.FDW.EF.Entities.Feedback;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Entities.Analysis;

namespace CSG.MI.FDW.EF
{
    public class ModelMappingProfile : Profile
	{
		public ModelMappingProfile()
		{
			CreateMap();
		}

		private void CreateMap()
		{
			#region RTLS Data Map

			CreateMap<TbErpPlanDataEntity, ErpPlan>().ReverseMap();
			CreateMap<TbEslLocationEntity, EslLocation>().ReverseMap();
			CreateMap<TbProcessLocEntity, ProcessMst>().ReverseMap();

			#endregion

			#region PCC Data Map

			CreateMap<MstOpcdEntity, OpMst>();
			CreateMap<MstPlanIssuecdEntity, IssueMst>().ReverseMap();
			CreateMap<MstMachineEntity, MachineMst>().ReverseMap();
			CreateMap<MstUserEntity, UserMst>().ReverseMap();
            CreateMap<MstComomEntity, CommonMst>().ReverseMap();

            CreateMap<BomHeadEntity, BomHead>().ReverseMap();
			CreateMap<PlanOpcdEntity, PlanOp>().ReverseMap();
			CreateMap<PlanStatusEntity, PlanStatus>().ReverseMap();
			CreateMap<ProdMaterialEntity, ProdMaterial>().ReverseMap();
			CreateMap<ProdScanEntity, ProdScan>().ReverseMap();
			CreateMap<IssueHeadEntity, IssueHead>().ReverseMap();
			CreateMap<IssueTailEntity, IssueTail>().ReverseMap();
			CreateMap<ReworkOpcdEntity, ReworkOp>().ReverseMap();
			CreateMap<ReworkHeadEntity, ReworkHead>().ReverseMap();
			CreateMap<ReworkDetailEntity, ReworkDetail>().ReverseMap();

            #endregion

            #region Production Data Map

            CreateMap<DailyStatusEntity, DailyStatus>().ReverseMap();
            CreateMap<WsEntity, Sample>()
				.ForMember(dest => dest.Factory, opt => opt.MapFrom(src => src.Factory))
				.ForMember(dest => dest.WsNo, opt => opt.MapFrom(src => src.WsNo))
				.ForMember(dest => dest.Detail, opt => opt.Ignore())
				.ForMember(dest => dest.Histories, opt => opt.Ignore())
				.ForMember(dest => dest.Coordinates, opt => opt.Ignore()).ReverseMap();
            CreateMap<WsSummaryEntity, SampleWork>().ReverseMap();
			CreateMap<WsHistoryEntity, SampleHist>().ReverseMap();
			CreateMap<WsDetailEntity, SampleDetail>().ReverseMap();
            CreateMap<WsCoodinateEntity, SampleCoordinate>().ReverseMap();
            CreateMap<WsCoordinateHistoryEntity, CoordinateHistory>().ReverseMap();
            CreateMap<WsRateEntity, WipRate>().ReverseMap();

            CreateMap<FactoryPlnTotalEntity, FactoryTotal>().ReverseMap();
			CreateMap<FactoryPrfTotalEntity, FactoryTotal>().ReverseMap();

            #endregion

            #region Analysis Data Map

            CreateMap<DevStylePredictionEntity, DevStylePrediction>().ReverseMap();

            #endregion

            #region Feedback Data Map

            //(Not-Used) CreateMap<StatusEntity, StautsMst>().ReverseMap();
            CreateMap<CategoryEntity, CategoryMst>().ReverseMap();
			CreateMap<FeedbackEntity, Feedback>().ReverseMap();

            #endregion

            #region Department Data Map

            CreateMap<DeptEntity, Dept>().ReverseMap();
			CreateMap<DeptRnREntity, DeptRnR>().ReverseMap();

			#endregion
		}
	}
}