using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface
{
    public interface IWsCoordinateRepository : IGenericMapRepository<WsCoodinateEntity, SampleCoordinate, HQDbContext>
    {
        #region Coordinates

        ICollection<SampleCoordinate> GetCoordinates();

        Task<ICollection<SampleCoordinate>> GetCoordinatesAsync();

        #endregion

        #region Coordinates History

        ICollection<SampleCoordinateHistory> GetHistory();

        Task<ICollection<SampleCoordinateHistory>> GetHistoryAsync();

        ICollection<SampleCoordinateHistory> GetHistoryBy(string wsno);

        Task<ICollection<SampleCoordinateHistory>> GetHistoryByAsync(string wsno);

        #endregion
    }
}
