using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace CSG.MI.TrMontrgSrv.WebApi.Infrastructure.Binders
{
    public class QueryBooleanModelBinderProvider : IModelBinderProvider
    {
        public IModelBinder GetBinder(ModelBinderProviderContext context)
        {
            if (context.Metadata.ModelType == typeof(bool) &&
                context.BindingInfo.BindingSource != null &&
                context.BindingInfo.BindingSource.CanAcceptDataFrom(BindingSource.Query))
            {
                return new QueryBooleanModelBinder();
            }

            return null;
        }
    }
}
