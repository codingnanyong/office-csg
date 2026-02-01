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
    public class EntityMappingProfile : Profile
	{
		public EntityMappingProfile()
		{
			CreateMap();
		}

		private void CreateMap()
		{
			#region RTLS Data Map

			CreateMap<TbErpPlanDataEntity, ErpPlan>();
			CreateMap<TbEslLocationEntity, EslLocation>();
			CreateMap<TbProcessLocEntity, ProcessMst>();

			#endregion

			#region PCC Data Map

			CreateMap<MstOpcdEntity, OpMst>();
			CreateMap<MstPlanIssuecdEntity, IssueMst>();
			CreateMap<MstMachineEntity, MachineMst>();
			CreateMap<MstUserEntity, UserMst>();
			CreateMap<MstComomEntity,CommonMst>();

			CreateMap<BomHeadEntity, BomHead>();
			CreateMap<PlanOpcdEntity, PlanOp>();
			CreateMap<PlanStatusEntity, PlanStatus>();
			CreateMap<ProdMaterialEntity, ProdMaterial>();
			CreateMap<ProdScanEntity, ProdScan>();
			CreateMap<IssueHeadEntity, IssueHead>();
			CreateMap<IssueTailEntity, IssueTail>();
			CreateMap<ReworkOpcdEntity, ReworkOp>();
			CreateMap<ReworkHeadEntity, ReworkHead>();
			CreateMap<ReworkDetailEntity, ReworkDetail>();

			#endregion

			#region Production Data Map

			CreateMap<DailyStatusEntity, DailyStatus>();
            CreateMap<WsEntity, Sample>()
				.ForMember(dest => dest.Factory, opt => opt.MapFrom(src => src.Factory))
				.ForMember(dest => dest.WsNo, opt => opt.MapFrom(src => src.WsNo))
				.ForMember(dest => dest.Detail, opt => opt.Ignore())
				.ForMember(dest => dest.Histories, opt => opt.Ignore())
				.ForMember(dest => dest.Coordinates, opt => opt.Ignore());
            CreateMap<WsSummaryEntity, SampleWork>();
			CreateMap<WsHistoryEntity, SampleHist>();
			CreateMap<WsDetailEntity, SampleDetail>();
			CreateMap<WsCoodinateEntity, SampleCoordinate>();
			CreateMap<WsCoordinateHistoryEntity, CoordinateHistory>();
			CreateMap<WsCoordinateHistoryEntity, SampleCoordinateHistory>();
			CreateMap<WsRateEntity,WipRate>();

			CreateMap<FactoryPlnTotalEntity, FactoryTotal>();
			CreateMap<FactoryPrfTotalEntity, FactoryTotal>();

            #endregion

            #region Analysis Data Map

            CreateMap<DevStylePredictionEntity, DevStylePrediction>();

            #endregion

            #region Feedback Data Map

            //(Not-Used) CreateMap<StatusEntity, StautsMst>();
            CreateMap<CategoryEntity, CategoryMst>();
			CreateMap<FeedbackEntity, Feedback>();

            #endregion

            #region Department Data Map

            CreateMap<DeptEntity, Dept>();
			CreateMap<DeptRnREntity, DeptRnR>();

			#endregion
		}
	}
}