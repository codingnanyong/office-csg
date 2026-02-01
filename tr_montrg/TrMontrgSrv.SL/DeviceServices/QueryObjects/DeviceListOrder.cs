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
    public enum OrderByOptions
    {
        [Display(Name = "Plant ↑")]
        ByPlantId,
        [Display(Name = "Device ↑")]
        ByDeviceId,
        [Display(Name = "Updated Date ↑")]
        ByUpdatedDate,
        [Display(Name = "Updated Date ↓")]
        ByUpdatedDateRecentFirst
    }

    public static class DeviceListOrder
    {
        public static IQueryable<DeviceEntity> OrderLocationsBy(this IQueryable<DeviceEntity> devices, OrderByOptions orderByOptions)
        {
            switch (orderByOptions)
            {
                case OrderByOptions.ByPlantId:
                    return devices.OrderBy(x => x.PlantId);
                case OrderByOptions.ByDeviceId:
                    return devices.OrderBy(x => x.DeviceId);
                case OrderByOptions.ByUpdatedDate:
                    return devices.OrderBy(x => x.UpdatedDt);
                case OrderByOptions.ByUpdatedDateRecentFirst:
                    return devices.OrderByDescending(x => x.UpdatedDt);
                default:
                    throw new ArgumentOutOfRangeException(nameof(orderByOptions), orderByOptions, null);
            }
        }
    }
}
