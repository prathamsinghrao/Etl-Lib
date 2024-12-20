﻿using System.Collections.Generic;
using EtlLib.Data;
using EtlLib.Pipeline;

namespace EtlLib.Nodes.Impl
{
    public class GenericMergeNode<T> : AbstractSourceNode<T>, ISinkNode2<T>
        where T : class, INodeOutput<T>, new()
    {
        public IEnumerable<T> Input { get; private set; }
        public IEnumerable<T> Input2 { get; private set; }

        public ISinkNode<T> SetInput(IEnumerable<T> input)
        {
            Input = input;
            return this;
        }
        
        public ISinkNode2<T> SetInput2(IEnumerable<T> input2)
        {
            Input2 = input2;
            return this;
        }

        public override void OnExecute(EtlPipelineContext context)
        {
            using (var input1Enumerator = Input.GetEnumerator())
            using (var input2Enumerator = Input2.GetEnumerator())
            {
                var input1HasItems = input1Enumerator.MoveNext();
                var input2HasItems = input2Enumerator.MoveNext();

                while (input1HasItems && input2HasItems)
                {
                    Emit(input1Enumerator.Current);
                    input1HasItems = input1Enumerator.MoveNext();

                    Emit(input2Enumerator.Current);
                    input2HasItems = input2Enumerator.MoveNext();
                }

                while (input1HasItems)
                {
                    Emit(input1Enumerator.Current);
                    input1HasItems = input1Enumerator.MoveNext();
                }

                while (input2HasItems)
                {
                    Emit(input2Enumerator.Current);
                    input2HasItems = input2Enumerator.MoveNext();
                }

                TypedEmitter.SignalEnd();
            }
        }
    }
}