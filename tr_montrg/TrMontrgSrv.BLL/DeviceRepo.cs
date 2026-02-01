using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL
{
    /// <summary>
    ///
    /// </summary>
    public class DeviceRepo : IDeviceRepo
    {
        private IDeviceRepository _deviceRepo;
        private IFrameRepository _frameRepo;
        private IRoiRepository _roiRepo;
        private IBoxRepository _boxRepo;
        private IMediumRepository _mediumRepo;

        public DeviceRepo(IDeviceRepository deviceRepo,
                          IFrameRepository frameRepo,
                          IRoiRepository roiRepo,
                          IBoxRepository boxRepo,
                          IMediumRepository mediumRepo)
        {
            _deviceRepo = deviceRepo;
            _frameRepo = frameRepo;
            _roiRepo = roiRepo;
            _boxRepo = boxRepo;
            _mediumRepo = mediumRepo;
        }

        public bool Exists(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _deviceRepo.Exists(deviceId);
        }

        public Device Get(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _deviceRepo.Get(deviceId);
        }

        public List<Device> GetAll()
        {
            return _deviceRepo.GetAll().ToList();
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="deviceId"></param>
        /// <param name="ymd"></param>
        /// <param name="hms"></param>
        /// <returns></returns>
        public Device GetSnap(string ymd, string hms, string deviceId)
        {
            var device = Get(deviceId);

            //device.Frames = new List<Frame>();
            device.Frames.Add(_frameRepo.Get(ymd, hms, deviceId));

            //device.Rois = new List<Roi>();
            device.Rois = _roiRepo.FindBy(ymd, hms, deviceId);

            //device.Boxes = new List<Box>();
            device.Boxes = _boxRepo.FindBy(ymd, hms, deviceId);

            //device.Media = new List<Medium>();
            device.Media.Add(_mediumRepo.Get(ymd, hms, deviceId, MediumType.rgb.ToString()));
            device.Media.Add(_mediumRepo.Get(ymd, hms, deviceId, MediumType.ir.ToString()));

            return device;
        }

        public ICollection<Device> FindBy(string plantId, string locationId = null)
        {
            if (String.IsNullOrWhiteSpace(plantId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            if (locationId == null)
                return _deviceRepo.FindBy(plantId);

            return _deviceRepo.FindBy(plantId, locationId);
        }

        public async Task<ICollection<Device>> FindByAsync(string plantId, string locationId = null)
        {
            if (String.IsNullOrWhiteSpace(plantId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            if (locationId == null)
                return await _deviceRepo.FindByAsync(plantId);

            return await _deviceRepo.FindByAsync(plantId, locationId);
        }

        public bool CreateAlways(Device device)
        {
            return _deviceRepo.CreateAlways(device);
        }

        public Device Create(Device device)
        {
            return _deviceRepo.Create(device);
        }

        public async Task<Device> CreateAsync(Device device)
        {
            return await _deviceRepo.CreateAsync(device);
        }

        public Device Update(Device device)
        {
            return _deviceRepo.Update(device);
        }

        public async Task<Device> UpdateAsync(Device device)
        {
            return await _deviceRepo.UpdateAsync(device);
        }

        public int Delete(string deviceId)
        {
            return _deviceRepo.Delete(deviceId);
        }

        public async Task<int> DeleteAsync(string deviceId)
        {
            return await _deviceRepo.DeleteAsync(deviceId);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_deviceRepo != null)
                {
                    _deviceRepo.Dispose();
                    _deviceRepo = null;
                }

                if (_frameRepo != null)
                {
                    _frameRepo.Dispose();
                    _frameRepo = null;
                }

                if (_roiRepo != null)
                {
                    _roiRepo.Dispose();
                    _roiRepo = null;
                }

                if (_boxRepo != null)
                {
                    _boxRepo.Dispose();
                    _boxRepo = null;
                }

                if (_mediumRepo != null)
                {
                    _mediumRepo.Dispose();
                    _mediumRepo = null;
                }
            }
        }
    }
}
