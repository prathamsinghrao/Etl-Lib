﻿using System.Collections.Generic;
using System.Data;
using EtlLib.Pipeline;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Nodes.PostgreSQL
{
    public class ExecutePostgresCommandOperation : AbstractEtlOperationWithNoResult
    {
        private readonly string _connectionString;
        private readonly string _commandText;
        private readonly Dictionary<string, object> _parameters;

        private IsolationLevel _isolationLevel;

        public ExecutePostgresCommandOperation(string name, string connectionString, string commandText)
        {
            Named(name);
            _connectionString = connectionString;
            _commandText = commandText;
            _parameters = new Dictionary<string, object>();
        }

        public ExecutePostgresCommandOperation WithParameter(string name, object value)
        {
            _parameters[name] = value;
            return this;
        }

        public ExecutePostgresCommandOperation WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public override IEtlOperationResult Execute(EtlPipelineContext context)
        {
            using (var con = new Npgsql.NpgsqlConnection(_connectionString))
            {
                con.Open();

                using (var trx = con.BeginTransaction(_isolationLevel))
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandText = _commandText;
                    cmd.CommandType = CommandType.Text;
                    cmd.Transaction = trx;

                    foreach (var param in _parameters)
                    {
                        var p = cmd.CreateParameter();
                        p.ParameterName = param.Key;
                        p.Value = param.Value;

                        cmd.Parameters.Add(p);
                    }

                    cmd.ExecuteNonQuery();
                }
            }

            return new EtlOperationResult(true);
        }
    }
}