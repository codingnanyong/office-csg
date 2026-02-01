using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace CSG.MI.TrMontrgSrv.Helpers.Extentions
{
    /// <summary>
    /// https://stackoverflow.com/questions/43483263/asp-net-core-entity-framework-sql-query-select
    /// </summary>
    public static class RDFacadeExtensions
    {
        /// <summary>
        ///
        /// </summary>
        /// <param name="databaseFacade"></param>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static RelationalDataReader ExecuteSqlQuery(this DatabaseFacade databaseFacade, string sql, params DbParameter[] parameters)
        {
            var concurrencyDetector = databaseFacade.GetService<IConcurrencyDetector>();

            using (concurrencyDetector.EnterCriticalSection())
            {
                var rawSqlCommand = databaseFacade.GetService<IRawSqlCommandBuilder>()
                                                  .Build(sql, parameters);

                var cmdParmObj = new RelationalCommandParameterObject(
                    connection: databaseFacade.GetService<IRelationalConnection>(),
                    parameterValues: rawSqlCommand.ParameterValues,
                    readerColumns: null,
                    context: null,
                    logger: null,
                    detailedErrorsEnabled: true
                );

                return rawSqlCommand.RelationalCommand.ExecuteReader(cmdParmObj);
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <example>
        /// SqlParameter[] parms = new SqlParameter[] { new SqlParameter("@ID", ID) };
        /// var result = RDFacadeExtensions.GetModelFromQuery<Object_List_VM>(context, "EXEC [sp_Object_GetList] @ID", parms);
        /// </example>
        /// <returns></returns>
        public static IEnumerable<T> GetModelFromQuery<T>(DbContext context, string sql, DbParameter[] parameters)
            where T : new()
        {
            DatabaseFacade databaseFacade = new (context);
            using (DbDataReader reader = databaseFacade.ExecuteSqlQuery(sql, parameters).DbDataReader)
            {
                List<T> list = new();
                PropertyInfo[] props = typeof(T).GetProperties();
                while (reader.Read())
                {
                    T t = new ();
                    IEnumerable<string> actualNames = reader.GetColumnSchema().Select(o => o.ColumnName);
                    for (int i = 0; i < props.Length; ++i)
                    {
                        PropertyInfo pi = props[i];
                        if (pi.CanWrite == false)
                            continue;
                        ColumnAttribute ca = pi.GetCustomAttribute(typeof(ColumnAttribute)) as ColumnAttribute;
                        string name = ca?.Name ?? pi.Name;
                        if (pi == null)
                            continue;

                        if (actualNames.Contains(name) == false)
                            continue;

                        object value = reader[name];
                        Type pt = pi.DeclaringType;
                        bool nullable = pt.GetTypeInfo().IsGenericType && pt.GetGenericTypeDefinition() == typeof(Nullable<>);

                        if (value == DBNull.Value)
                            value = null;

                        if (value == null && pt.GetTypeInfo().IsValueType && !nullable)
                            value = Activator.CreateInstance(pt);

                        pi.SetValue(t, value);
                    }

                    list.Add(t);
                }

                return list;
            }
        }
    }
}
