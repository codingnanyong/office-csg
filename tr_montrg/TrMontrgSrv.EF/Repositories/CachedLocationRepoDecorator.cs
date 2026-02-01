using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class CachedLocationRepoDecorator : IReadOnlyGenericMapRepository<DeviceEntity, Device>, IDisposable
    {
        #region Fields

        private readonly DeviceRepository _repo;
        private readonly IMemoryCache _cache;
        private const string LOCATION_CACHE_KEY = "LocationEntity";
        private readonly MemoryCacheEntryOptions _cacheOptions;
        private bool _disposed = false;

        #endregion

        #region Constructors

        public CachedLocationRepoDecorator(DeviceRepository repo, IMemoryCache cache)
        {
            _repo = repo;
            _cache = cache;

            _cacheOptions = new MemoryCacheEntryOptions()
                .SetAbsoluteExpiration(relative: TimeSpan.FromSeconds(Constants.DEFAULT_CACHE_SECONDS));
        }

        #endregion

        #region Public Methods

        #region Count

        public int Count()
        {
            return GetAll().Count;
        }

        public async Task<int> CountAsync()
        {
            var all = await GetAllAsync();
            return all.Count;
        }

        #endregion

        #region Get

        public Device Get(params object[] keys)
        {
            if (keys.Length < 3)
                return null;

            return GetAll().FirstOrDefault(x => x.PlantId == (string)keys[0] &&
                                                x.LocationId == (string)keys[1] &&
                                                x.DeviceId == (string)keys[2]);
        }

        public async Task<Device> GetAsync(params object[] keys)
        {
            if (keys.Length < 3)
                return null;

            var all = await GetAllAsync();
            return all.AsQueryable().FirstOrDefault(x => x.PlantId == (string)keys[0] &&
                                                         x.LocationId == (string)keys[1] &&
                                                         x.DeviceId == (string)keys[2]);
        }

        public ICollection<Device> GetAll()
        {
            var entities = GetEntityAll();

            //return _repo.Context.Mapper.Map<IQueryable<Device>>(entities);
            return _repo.Context.Mapper.Map<ICollection<Device>>(entities.ToList());
        }

        public async Task<ICollection<Device>> GetAllAsync()
        {
            var entities = await GetEntityAllAsync();

            return _repo.Context.Mapper.Map<ICollection<Device>>(entities);
        }

        public IQueryable<Device> GetAllIncluding(params Expression<Func<DeviceEntity, object>>[] includeProperties)
        {
            IQueryable<DeviceEntity> queryable = GetEntityAll();
            foreach (Expression<Func<DeviceEntity, object>> includeProperty in includeProperties)
            {
                queryable = queryable.Include<DeviceEntity, object>(includeProperty);
            }

            return _repo.Context.Mapper.Map<IQueryable<Device>>(queryable);
        }

        #endregion

        #region Find

        public Device Find(Expression<Func<DeviceEntity, bool>> match)
        {
            var entity = GetEntityAll().SingleOrDefault(match);
            return _repo.Context.Mapper.Map<Device>(entity);
        }

        public async Task<Device> FindAsync(Expression<Func<DeviceEntity, bool>> match)
        {
            var all = await GetEntityAllAsync();
            var entity = all.AsQueryable().SingleOrDefault(match);
            return _repo.Context.Mapper.Map<Device>(entity);
        }

        public ICollection<Device> FindBy(Expression<Func<DeviceEntity, bool>> predicate)
        {
            IQueryable<DeviceEntity> query = GetEntityAll().Where(predicate);
            //return _repo.Context.Mapper.Map<IQueryable<Device>>(query);
            return _repo.Context.Mapper.Map<ICollection<Device>>(query.ToList());
        }

        public async Task<ICollection<Device>> FindByAsync(Expression<Func<DeviceEntity, bool>> predicate)
        {
            var all = await GetEntityAllAsync();
            var entities = all.AsQueryable().Where(predicate).ToList();
            return _repo.Context.Mapper.Map<ICollection<Device>>(entities);
        }

        public ICollection<Device> FindAll(Expression<Func<DeviceEntity, bool>> match)
        {
            var entities = GetEntityAll().Where(match).ToList();
            return _repo.Context.Mapper.Map<ICollection<Device>>(entities);
        }

        public async Task<ICollection<Device>> FindAllAsync(Expression<Func<DeviceEntity, bool>> match)
        {
            var all = await GetEntityAllAsync();
            var entities = all.AsQueryable().Where(match).ToList();
            return _repo.Context.Mapper.Map<ICollection<Device>>(entities);
        }

        #endregion

        #endregion // Public Methods

        #region Private Methods

        private IQueryable<DeviceEntity> GetEntityAll()
        {
            var entities = _cache.GetOrCreate(LOCATION_CACHE_KEY, entry =>
            {
                entry.SetOptions(_cacheOptions);
                return _repo.Context.Set<DeviceEntity>();
            });

            return entities;
        }

        private async Task<ICollection<DeviceEntity>> GetEntityAllAsync()
        {
            var entities = await _cache.GetOrCreate(LOCATION_CACHE_KEY, entry =>
            {
                entry.SetOptions(_cacheOptions);
                return _repo.Context.Set<DeviceEntity>().ToListAsync();
            });

            return entities;
        }

        #endregion

        #region Disposable

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            // Dispose managed objects.
            if (disposing)
            {
                _repo?.Dispose();
                _cache?.Dispose();
            }

            // Free unmanaged resources and override a finalizer below.
            // Set large fields to null.

            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
