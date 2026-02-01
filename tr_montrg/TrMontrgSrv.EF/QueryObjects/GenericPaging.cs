using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.EF.QueryObjects
{
    public static class GenericPaging
    {
        public static IQueryable<T> Page<T>(this IQueryable<T> query, int pageNumZeroStart, int pageSize)
        {
            if (pageSize == 0)
                throw new ArgumentOutOfRangeException(nameof(pageSize), "pageSize cannot be zero.");

            if (pageNumZeroStart != 0)
                query = query.Skip(pageNumZeroStart * pageSize);

            return query.Take(pageSize);
        }
    }
}
