using CSG.MI.DTO.Production;

namespace CSG.MI.FDW.BLL.Production.DataMart.Interface
{
    public interface IWsCoordinateRepo : IDisposable
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
