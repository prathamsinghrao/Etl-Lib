﻿using System;
using System.Collections.Generic;
using EtlLib.Pipeline.Builders;
using EtlLib.Pipeline.Operations;

namespace EtlLib.Pipeline
{
    public interface IEtlPipeline : IEtlOperationCollection, IEtlOperation
    {
        EtlPipelineContext Context { get; }
        IEtlOperationResult LastResult { get; }

        EtlPipelineResult Execute();

        IEtlPipeline Run(Action<EtlPipelineContext, IEtlProcessBuilder> builder);
        IEtlPipeline Run(IEtlOperation operation);
        IEtlPipeline Run(Func<EtlPipelineContext, IEtlOperation> ctx);

        IEtlPipeline Run<TOut>(IEtlOperationWithEnumerableResult<TOut> operation, 
            Action<IEtlPipelineEnumerableResultContext<TOut>> result);
        IEtlPipeline Run<TOut>(Func<EtlPipelineContext, IEtlOperationWithEnumerableResult<TOut>> operation,
            Action<IEtlPipelineEnumerableResultContext<TOut>> result);

        IEtlPipeline Run<TOut>(IEtlOperationWithScalarResult<TOut> operation,
            Action<IEtlPipelineWithScalarResultContext<TOut>> result);
        IEtlPipeline Run<TOut>(Func<EtlPipelineContext, IEtlOperationWithScalarResult<TOut>> operation,
            Action<IEtlPipelineWithScalarResultContext<TOut>> result);

        IEtlPipelineEnumerableResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithEnumerableResult<TOut> operation);
        IEtlPipelineEnumerableResultContext<TOut> RunWithResult<TOut>(Func<EtlPipelineContext, 
            IEtlOperationWithEnumerableResult<TOut>> operation);
        IEtlPipelineWithScalarResultContext<TOut> RunWithResult<TOut>(IEtlOperationWithScalarResult<TOut> operation);

        IEtlPipeline RunParallel(Func<EtlPipelineContext, IEnumerable<IEtlOperation>> ctx);

        IEtlPipeline RunIf(Func<EtlPipelineContext, bool> predicate, Func<EtlPipelineContext, IEtlOperation> operation);
        
        IEtlPipeline If(Func<EtlPipelineContext, bool> predicate, Action<IEtlPipeline> pipeline);
        IDoWhileEtlOperationContext Do(Action<IEtlPipeline> pipeline);
    }
}