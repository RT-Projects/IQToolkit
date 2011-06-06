// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace IQToolkit.Data.Postgres
{
    using IQToolkit.Data.Common;
    using Npgsql;
    using NpgsqlTypes;

    public class PostgresQueryProvider : DbEntityProvider
    {
        public PostgresQueryProvider(NpgsqlConnection connection, QueryMapping mapping, QueryPolicy policy)
            : base(connection, PostgresLanguage.Default, mapping, policy)
        {
        }

        public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return new PostgresQueryProvider((PostgresConnection) connection, mapping, policy);
        }

        public static string GetConnectionString(string databaseName)
        {
            return string.Format(@"Server=127.0.0.1;Database={0}", databaseName);
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        new class Executor : DbEntityProvider.Executor
        {
            PostgresQueryProvider provider;

            public Executor(PostgresQueryProvider provider)
                : base(provider)
            {
                this.provider = provider;
            }

            protected override bool BufferResultRows
            {
                get { return true; }
            }

            protected override void AddParameter(DbCommand command, QueryParameter parameter, object value)
            {
                DbQueryType sqlType = (DbQueryType)parameter.QueryType;
                if (sqlType == null)
                    sqlType = (DbQueryType)this.provider.Language.TypeSystem.GetColumnType(parameter.Type);
                var p = ((NpgsqlCommand) command).Parameters.Add(parameter.Name, ToPostgresDbType(sqlType.SqlDbType), sqlType.Length);
                if (sqlType.Precision != 0)
                    p.Precision = (byte)sqlType.Precision;
                if (sqlType.Scale != 0)
                    p.Scale = (byte)sqlType.Scale;
                p.Value = value ?? DBNull.Value;
            }
        }

        public static NpgsqlDbType ToPostgresDbType(SqlDbType dbType)
        {
            switch (dbType)
            {
                case SqlDbType.BigInt:
                    return NpgsqlDbType.Bigint;
                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.VarBinary:
                    return NpgsqlDbType.Bytea;
                case SqlDbType.Bit:
                    return NpgsqlDbType.Bit;
                case SqlDbType.NChar:
                case SqlDbType.Char:
                    return NpgsqlDbType.Text;
                case SqlDbType.Date:
                    return NpgsqlDbType.Date;
                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                    return NpgsqlDbType.Timestamp;
                case SqlDbType.Float:
                    return NpgsqlDbType.Real;
                case SqlDbType.Int:
                    return NpgsqlDbType.Integer;
                case SqlDbType.Decimal:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return NpgsqlDbType.Money;
                case SqlDbType.NVarChar:
                case SqlDbType.VarChar:
                    return NpgsqlDbType.Text;
                case SqlDbType.SmallInt:
                    return NpgsqlDbType.Smallint;
                case SqlDbType.NText:
                case SqlDbType.Text:
                    return NpgsqlDbType.Text;
                case SqlDbType.Time:
                    return NpgsqlDbType.Time;
                case SqlDbType.Timestamp:
                    return NpgsqlDbType.Timestamp;
                //case SqlDbType.TinyInt:
                //    return NpgsqlDbType.Byte;
                case SqlDbType.UniqueIdentifier:
                    return NpgsqlDbType.Uuid;
                case SqlDbType.Xml:
                    return NpgsqlDbType.Text;
                default:
                    throw new NotSupportedException(string.Format("The SQL type '{0}' is not supported", dbType));
            }
        }
    }
}
