﻿using System;
using System.Data;
using Dapper;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Dapper
{
    public class DapperReaderNode<TOut> : AbstractSourceNode<TOut> 
        where TOut : class, INodeOutput<TOut>, new()
    {
        private readonly string _connectionName;
        private Func<IDbConnection> _createConnection;
        private readonly string _sql;
        private readonly object _param;
        private IsolationLevel _isolationLevel;
        private bool _buffered;
        private int? _timeoutInSeconds;
        private DynamicParameters _dynamicParameters;
        private CommandType _commandType;

        protected DapperReaderNode(string command, object param = null)
        {
            _sql = command;
            _param = param;
            _isolationLevel = IsolationLevel.ReadCommitted;
            _buffered = true;
            _commandType = CommandType.Text;
        }

        public DapperReaderNode(Func<IDbConnection> createConnection, string command, object param = null)
            : this(command, param)
        {
            _createConnection = createConnection;
        }

        public DapperReaderNode(string connectionName, string command, object param = null)
            : this(command, param)
        {
            _connectionName = connectionName;
        }

        public DapperReaderNode<TOut> WithIsolationLevel(IsolationLevel isolationLevel)
        {
            _isolationLevel = isolationLevel;
            return this;
        }

        public DapperReaderNode<TOut> WithBuffer(bool bufferResults)
        {
            _buffered = bufferResults;
            return this;
        }

        public DapperReaderNode<TOut> WithTimeout(TimeSpan timeout)
        {
            _timeoutInSeconds = (int)timeout.TotalSeconds;
            return this;
        }

        public DapperReaderNode<TOut> WithDynamicParameters(Action<DynamicParameters> param)
        {
            _dynamicParameters = new DynamicParameters();
            param(_dynamicParameters);
            return this;
        }

        public DapperReaderNode<TOut> WithCommandType(CommandType commandType)
        {
            _commandType = commandType;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            if (_createConnection == null)
            {
                _createConnection = () => context.DbConnectionFactory.CreateNamedConnection(_connectionName);
            }

            using (var con = _createConnection())
            {
                if (con.State != ConnectionState.Open)
                    con.Open();

                using (var trx = con.BeginTransaction(_isolationLevel))
                {
                    foreach (var result in con.Query<TOut>(_sql, _dynamicParameters ?? _param, trx, _buffered,
                        _timeoutInSeconds, _commandType))
                    {
                        Emit(result);
                    }
                }
            }

            SignalEnd(); //TODO: SignalEnd() really could be called by the task instead straight to the IOAdapter when Execute() returns?
        }
    }
}
