﻿using System;
using System.Collections.Generic;
using System.Data;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class SimpleDbDataReaderNode : AbstractSourceNode<Row>
    {
        private readonly string _connectionName;
        private IsolationLevel _isolationLevel;
        private CommandType _commandType;
        private readonly string _commandText;
        private readonly Dictionary<string, object> _parameters;


        public SimpleDbDataReaderNode(string connectionName, string commandText)
        {
            _connectionName = connectionName;
            _commandText = commandText;
            _parameters = new Dictionary<string, object>();
            _isolationLevel = IsolationLevel.ReadCommitted;
            _commandType = CommandType.Text;
        }     

        public SimpleDbDataReaderNode WithCommandType(CommandType commandType)
        {
            _commandType = commandType;
            return this;
        }

        public SimpleDbDataReaderNode WithParameter(string name, object value)
        {
            _parameters[name] = value;
            return this;
        }

        public SimpleDbDataReaderNode WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            var logger = context.GetLogger(GetType().FullName);

            try
            {
                using (var con = context.CreateNamedDbConnection(_connectionName))
                {
                    if (con.State != ConnectionState.Open)
                        con.Open();

                    using (var trx = con.BeginTransaction(_isolationLevel))
                    using (var cmd = con.CreateCommand())
                    {
                        cmd.CommandText = _commandText;
                        cmd.CommandType = _commandType;
                        cmd.Transaction = trx;

                        foreach (var param in _parameters)
                        {
                            var p = cmd.CreateParameter();
                            p.ParameterName = param.Key;
                            p.Value = param.Value;

                            cmd.Parameters.Add(p);
                        }

                        using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                        {
                            while (reader.Read())
                            {
                                var row = new Row();
                                for (var i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = reader[i];
                                }

                                Emit(row);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error($"Error while executing query: \"{_commandText}\"", e);
                throw;
            }
            finally
            {
                SignalEnd();
            }
        }
    }
}