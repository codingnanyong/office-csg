using AutoMapper;
using System.Linq.Expressions;

namespace CSG.MI.FDW.EF.Core.Extentions
{
	public static class AutoMapperExtensions
	{
		public static IMappingExpression<TSource, TDestination> IgnoreMember<TSource, TDestination>(
			this IMappingExpression<TSource, TDestination> map,
			Expression<Func<TDestination, object>> selector)
		{
			map.ForMember(selector, config => config.Ignore());
			return map;
		}
	}
}