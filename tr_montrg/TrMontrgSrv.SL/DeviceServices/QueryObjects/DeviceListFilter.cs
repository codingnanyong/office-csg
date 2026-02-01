using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core;
using CSG.MI.TrMontrgSrv.EF.Entities;
//using CS.MI.TrMontrgSrv.Model;

namespace CS.MI.TrMontrgSrv.EF.QueryObjects
{
    public enum LocationFilterBy
    {
        [Display(Name = "All")]
        NoFilter,
        [Display(Name = "By Plants...")]
        ByPlantId,
        [Display(Name = "By Locations...")]
        ByLocationId,
        [Display(Name = "By Devices...")]
        ByDeviceId
    }

    public static class DeviceListFilter
    {
        public static IQueryable<DeviceEntity> FilterLocationsBy(this IQueryable<DeviceEntity> devices,
                                                                   LocationFilterBy filterBy,
                                                                   string filterValue)
        {
            if (String.IsNullOrEmpty(filterValue))
                return devices;

            switch (filterBy)
            {
                case LocationFilterBy.NoFilter:
                    return devices;
                case LocationFilterBy.ByPlantId:
                    return devices.Where(x => x.PlantId == filterValue);
                case LocationFilterBy.ByLocationId:
                    return devices.Where(x => x.LocationId == filterValue);
                case LocationFilterBy.ByDeviceId:
                    return devices.Where(x => x.DeviceId == filterValue);
                default:
                    throw new ArgumentOutOfRangeException(nameof(filterBy), filterBy, null);
            }
        }

    }
}
