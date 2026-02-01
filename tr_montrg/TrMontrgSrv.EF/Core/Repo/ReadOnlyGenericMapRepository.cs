using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using CSG.MI.TrMontrgSrv.EF.Entities;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Core.Repo
{
    public class ReadOnlyGenericMapRepository<E, M> : IReadOnlyGenericMapRepository<E, M> where E : class
                                                                                          where M : class
    {
        #region Fields

        protected readonly AppDbContext _context;

        #endregion

        #region Constructors

        public ReadOnlyGenericMapRepository(AppDbContext ctx)
        {
            _context = ctx;
        }

        #endregion

        #region Properties

        public AppDbContext Context
        {
            get { return _context; }
        }

        protected bool IsDisposed { get; private set; }

        #endregion

        #region Public Methods

        #region Count

        public int Count() => _context.Set<E>().Count();

        public async Task<int> CountAsync() => await _context.Set<E>().CountAsync();

        #endregion

        #region Get

        public virtual M Get(params object[] keys)
        {
            E entity = _context.Set<E>().Find(keys);
            return _context.Mapper.Map<M>(entity);
        }

        public virtual async Task<M> GetAsync(params object[] keys)
        {
            E entity = await _context.Set<E>().FindAsync(keys);
            return _context.Mapper.Map<M>(entity);
        }

        public ICollection<M> GetAll()
        {
            IQueryable<E> queryable = _context.Set<E>();
            Debug.WriteLine(queryable.ToQueryString());
            // ERROR: Microsoft.EntityFrameworkCore.Query: Warning: Compiling a query
            // which loads related collections for more than one collection navigation either via 'Include' or
            // through projection but no 'QuerySplittingBehavior' has been configured.
            // By default Entity Framework will use 'QuerySplittingBehavior.SingleQuery'
            // which can potentially result in slow query performance.
            //return queryable.ProjectTo<M>(_context.Mapper.ConfigurationProvider);
            return _context.Mapper.Map<ICollection<M>>(queryable.ToList());
        }

        public virtual async Task<ICollection<M>> GetAllAsync()
        {
            var entities = await _context.Set<E>().ToListAsync();
            return _context.Mapper.Map<ICollection<M>>(entities);
        }

        public IQueryable<M> GetAllIncluding(params Expression<Func<E, object>>[] includeProperties)
        {
            IQueryable<E> queryable = _context.Set<E>();
            foreach (Expression<Func<E, object>> includeProperty in includeProperties)
            {
                queryable = queryable.Include<E, object>(includeProperty);
            }

            //return _context.Mapper.Map<IQueryable<M>>(queryable);
            return queryable.ProjectTo<M>(_context.Mapper.ConfigurationProvider);
        }

        //public IQueryable<M> GetAllExcluding(params Expression<Func<E, object>>[] includeProperties)
        //{
        //    IQueryable<E> queryable = _context.Set<E>();
        //    foreach (Expression<Func<E, object>> includeProperty in includeProperties)
        //    {
        //        queryable = queryable.exc<E, object>(includeProperty);
        //    }

        //    //return _context.Mapper.Map<IQueryable<M>>(queryable);
        //    return queryable.ProjectTo<M>(_context.Mapper.ConfigurationProvider);
        //}

        #endregion

        #region Find

        public virtual M Find(Expression<Func<E, bool>> match)
        {
            var query = _context.Set<E>().Where(match);
            Debug.WriteLine(query.ToQueryString());
            var entity = query.SingleOrDefault();
            //var entity = _context.Set<E>().SingleOrDefault();
            return _context.Mapper.Map<M>(entity);
        }

        public virtual async Task<M> FindAsync(Expression<Func<E, bool>> match)
        {
            var query = _context.Set<E>().Where(match);
            Debug.WriteLine(query.ToQueryString());
            var entity = await query.SingleOrDefaultAsync();
            //var entity = await _context.Set<E>().SingleOrDefaultAsync();
            return _context.Mapper.Map<M>(entity);
        }

        public virtual ICollection<M> FindBy(Expression<Func<E, bool>> predicate)
        {
            var query = _context.Set<E>().Where(predicate);
            Debug.WriteLine(query.ToQueryString());

            // ERROR: Microsoft.EntityFrameworkCore.Query: Warning: Compiling a query
            // which loads related collections for more than one collection navigation either via 'Include' or
            // through projection but no 'QuerySplittingBehavior' has been configured.
            // By default Entity Framework will use 'QuerySplittingBehavior.SingleQuery'
            // which can potentially result in slow query performance.
            //return query.ProjectTo<M>(_context.Mapper.ConfigurationProvider);
            return _context.Mapper.Map<ICollection<M>>(query.ToList());
        }

        public virtual async Task<ICollection<M>> FindByAsync(Expression<Func<E, bool>> predicate)
        {
            var query = _context.Set<E>().Where(predicate);
            Debug.WriteLine(query.ToQueryString());
            var entities = await query.ToListAsync();
            return _context.Mapper.Map<ICollection<M>>(entities);
        }

        public ICollection<M> FindAll(Expression<Func<E, bool>> match)
        {
            var query = _context.Set<E>().Where(match);
            Debug.WriteLine(query.ToQueryString());
            var entities = query.ToList();
            return _context.Mapper.Map<ICollection<M>>(entities);
        }

        public async Task<ICollection<M>> FindAllAsync(Expression<Func<E, bool>> match)
        {
            var query = _context.Set<E>().Where(match);
            Debug.WriteLine(query.ToQueryString());
            var entities = await query.ToListAsync();
            return _context.Mapper.Map<ICollection<M>>(entities);
        }

        #endregion

        #endregion // Public Methods

        #region Disposable

        protected virtual void Dispose(bool disposing)
        {
            if (IsDisposed)
                return;

            // Dispose managed objects.
            if (disposing)
            {
                //_context?.Dispose();
            }

            // Free unmanaged resources and override a finalizer below.
            // Set large fields to null.

            IsDisposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            //GC.SuppressFinalize(this);
        }

        #endregion
    }
}
