// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Reflection;

namespace IQToolkit
{
    /// <summary>
    /// Creates a reusable, parameterized representation of a query that caches the execution plan
    /// </summary>
    public static class QueryCompiler
    {
        public static Delegate Compile(LambdaExpression query)
        {
            CompiledQuery cq = new CompiledQuery(query);
            return StrongDelegate.CreateDelegate(query.Type, (Func<object[], object>)cq.Invoke);
        }

        public static D Compile<D>(Expression<D> query)
        {
            return (D)(object)Compile((LambdaExpression)query);
        }

        public static Func<TResult> Compile<TResult>(Expression<Func<TResult>> query)
        {
            return new CompiledQuery(query).Invoke<TResult>;
        }

        public static Func<T1, TResult> Compile<T1, TResult>(Expression<Func<T1, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, TResult>;
        }

        public static Func<T1, T2, TResult> Compile<T1, T2, TResult>(Expression<Func<T1, T2, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, T2, TResult>;
        }

        public static Func<T1, T2, T3, TResult> Compile<T1, T2, T3, TResult>(Expression<Func<T1, T2, T3, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, T2, T3, TResult>;
        }

        public static Func<T1, T2, T3, T4, TResult> Compile<T1, T2, T3, T4, TResult>(Expression<Func<T1, T2, T3, T4, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, T2, T3, T4, TResult>;
        }

        public static Func<IEnumerable<T>> Compile<T>(this IQueryable<T> source)
        {
            return Compile<IEnumerable<T>>(
                Expression.Lambda<Func<IEnumerable<T>>>(((IQueryable)source).Expression)
                );
        }

        public class CompiledQuery
        {
            LambdaExpression query;
            Delegate fnQuery;

            internal CompiledQuery(LambdaExpression query)
            {
                this.query = query;
            }

            public LambdaExpression Query
            {
                get { return this.query; }
            }

            internal void Compile(params object[] args)
            {
                if (this.fnQuery == null)
                {
                    // first identify the query provider being used
                    Expression body = this.query.Body;

                    // ask the query provider to compile the query by 'executing' the lambda expression
                    IQueryProvider provider = this.FindProvider(body, args);
                    if (provider == null)
                    {
                        throw new InvalidOperationException("Could not find query provider");
                    }

                    Delegate result = (Delegate)provider.Execute(this.query);
                    System.Threading.Interlocked.CompareExchange(ref this.fnQuery, result, null);
                }
            }

            internal IQueryProvider FindProvider(Expression expression, object[] args)
            {
                Expression root = this.FindProviderInExpression(expression);
                if (!(root is ConstantExpression) && args != null && args.Length > 0)
                {
                    Expression replaced = ExpressionReplacer.ReplaceAll(
                        expression,
                        this.query.Parameters.ToArray(),
                        args.Select((a, i) => Expression.Constant(a, this.query.Parameters[i].Type)).ToArray()
                        );
                    root = this.FindProviderInExpression(replaced);
                }
                if (root != null) 
                {
                    ConstantExpression cex = root as ConstantExpression;
                    if (cex == null)
                    {
                        cex = PartialEvaluator.Eval(root) as ConstantExpression;
                    }
                    if (cex != null)
                    {
                        IQueryProvider provider = cex.Value as IQueryProvider;
                        if (provider == null)
                        {
                            IQueryable query = cex.Value as IQueryable;
                            if (query != null)
                            {
                                provider = query.Provider;
                            }
                        }
                        return provider;
                    }
                }
                return null;
            }

            private Expression FindProviderInExpression(Expression expression)
            {
                Expression root = TypedSubtreeFinder.Find(expression, typeof(IQueryProvider));
                if (root == null)
                {
                    root = TypedSubtreeFinder.Find(expression, typeof(IQueryable));
                }
                return root;
            }

            public object Invoke(object[] args)
            {
                this.Compile(args);
                if (invoker == null)
                {
                    invoker = GetInvoker();
                }
                if (invoker != null)
                {
                    return invoker(args);
                }
                else
                {
                    try
                    {
                        return this.fnQuery.DynamicInvoke(args);
                    }
                    catch (TargetInvocationException tie)
                    {
                        throw tie.InnerException;
                    }
                }
            }

            Func<object[], object> invoker;
            bool checkedForInvoker;

            private Func<object[], object> GetInvoker()
            {
                if (this.fnQuery != null && this.invoker == null && !checkedForInvoker)
                {
                    this.checkedForInvoker = true;
                    Type fnType = this.fnQuery.GetType();
                    if (fnType.FullName.StartsWith("System.Func`"))
                    {
                        var typeArgs = fnType.GetGenericArguments();
                        MethodInfo method = this.GetType().GetMethod("FastInvoke"+typeArgs.Length, BindingFlags.Public|BindingFlags.Instance);
                        if (method != null)
                        {
                            this.invoker = (Func<object[], object>)Delegate.CreateDelegate(typeof(Func<object[], object>), this, method.MakeGenericMethod(typeArgs));
                        }
                    }
                }
                return this.invoker;
            }

            public object FastInvoke1<R>(object[] args)
            {
                return ((Func<R>)this.fnQuery)();
            }

            public object FastInvoke2<A1, R>(object[] args)
            {
                return ((Func<A1, R>)this.fnQuery)((A1)args[0]);
            }

            public object FastInvoke3<A1, A2, R>(object[] args)
            {
                return ((Func<A1, A2, R>)this.fnQuery)((A1)args[0], (A2)args[1]);
            }

            public object FastInvoke4<A1, A2, A3, R>(object[] args)
            {
                return ((Func<A1, A2, A3, R>)this.fnQuery)((A1)args[0], (A2)args[1], (A3)args[2]);
            }

            public object FastInvoke5<A1, A2, A3, A4, R>(object[] args)
            {
                return ((Func<A1, A2, A3, A4, R>)this.fnQuery)((A1)args[0], (A2)args[1], (A3)args[2], (A4)args[3]);
            }

            internal TResult Invoke<TResult>()
            {
                this.Compile(null);
                return ((Func<TResult>)this.fnQuery)();
            }

            internal TResult Invoke<T1, TResult>(T1 arg)
            {
                this.Compile(arg);
                return ((Func<T1, TResult>)this.fnQuery)(arg);
            }

            internal TResult Invoke<T1, T2, TResult>(T1 arg1, T2 arg2)
            {
                this.Compile(arg1, arg2);
                return ((Func<T1, T2, TResult>)this.fnQuery)(arg1, arg2);
            }

            internal TResult Invoke<T1, T2, T3, TResult>(T1 arg1, T2 arg2, T3 arg3)
            {
                this.Compile(arg1, arg2, arg3);
                return ((Func<T1, T2, T3, TResult>)this.fnQuery)(arg1, arg2, arg3);
            }

            internal TResult Invoke<T1, T2, T3, T4, TResult>(T1 arg1, T2 arg2, T3 arg3, T4 arg4)
            {
                this.Compile(arg1, arg2, arg3, arg4);
                return ((Func<T1, T2, T3, T4, TResult>)this.fnQuery)(arg1, arg2, arg3, arg4);
            }
        }
    }

    /// <summary>Base class for compiled queries. See Remarks for various conventions and notes.</summary>
    /// <remarks>
    /// <para>The compiled query must either capture a local that evaluates to the IQueryable being queried, or supply an IQueryProvider upon compilation.
    /// In the former scenario, the compiled query will only work on the given connection. In the latter scenario, to support a separate connection being
    /// used the IQueryable operated on should be one of the arguments taken by the query. Note that violating the former requirement may actually
    /// work on some providers, e.g. sqlite, but this should not be relied upon.</para>
    /// <para>This class and descendants are an alternative implementation to the one in <see cref="QueryCompiler"/>.</para>
    /// </remarks>
    public abstract class CompiledQuery
    {
        public static CompiledQuery<TResult> Create<TResult>(Expression<Func<TResult>> query) { return new CompiledQuery<TResult>(query); }
        public static CompiledQuery<T, TResult> Create<T, TResult>(Expression<Func<T, TResult>> query) { return new CompiledQuery<T, TResult>(query); }

        /// <summary>Compiles the <see cref="_Query"/> and places the result into <see cref="_Compiled"/>, unless there already is a result in <see cref="_Compiled"/>.</summary>
        /// <param name="args">If not null, the arguments will be searched for a query provider in case the expression doesn't have one as a ConstantExpression.</param>
        protected static Delegate Compile(LambdaExpression query, IQueryProvider provider, object[] args)
        {
            // attempt to locate a provider
            if (provider == null)
                provider = findProvider(query.Body, args, query.Parameters.ToArray());
            if (provider == null)
                throw new InvalidOperationException("Could not find query provider");

            // ask the query provider to compile the query by 'executing' the lambda expression
            return (Delegate) provider.Execute(query);
        }

        private static IQueryProvider findProvider(Expression expression, object[] args, ParameterExpression[] @params)
        {
            Expression root = findProviderInExpression(expression);
            if (!(root is ConstantExpression) && args != null && args.Length > 0)
            {
                Expression replaced = ExpressionReplacer.ReplaceAll(
                    expression,
                    @params,
                    args.Select((a, i) => Expression.Constant(a, @params[i].Type)).ToArray()
                    );
                root = findProviderInExpression(replaced);
            }
            if (root != null)
            {
                ConstantExpression cex = root as ConstantExpression;
                if (cex == null)
                {
                    cex = PartialEvaluator.Eval(root) as ConstantExpression;
                }
                if (cex != null)
                {
                    IQueryProvider provider = cex.Value as IQueryProvider;
                    if (provider == null)
                    {
                        IQueryable query = cex.Value as IQueryable;
                        if (query != null)
                        {
                            provider = query.Provider;
                        }
                    }
                    return provider;
                }
            }
            return null;
        }

        private static Expression findProviderInExpression(Expression expression)
        {
            Expression root = TypedSubtreeFinder.Find(expression, typeof(IQueryProvider));
            if (root == null)
            {
                root = TypedSubtreeFinder.Find(expression, typeof(IQueryable));
            }
            return root;
        }
    }

    /// <summary>Encapsulates a compiled query taking no arguments and returning a result.</summary>
    /// <typeparam name="TResult">Type of the query result.</typeparam>
    public sealed class CompiledQuery<TResult> : CompiledQuery
    {
        private Expression<Func<TResult>> _query;
        private Func<TResult> _compiled;

        /// <summary>Constructor.</summary>
        /// <param name="query">The query to be compiled, as an expression.</param>
        public CompiledQuery(Expression<Func<TResult>> query)
        {
            if (query == null) throw new ArgumentNullException();
            _query = query;
        }

        private void compile(IQueryProvider provider, object[] args)
        {
            var compiled = CompiledQuery.Compile(_query, provider, args);
            System.Threading.Interlocked.CompareExchange(ref _compiled, (Func<TResult>) compiled, null);
        }

        /// <summary>Compiles the query (unless it's already been compiled earlier). Returns a delegate that represents the compiled query.</summary>
        /// <param name="provider">In cases where the query expression itself doesn't reference a queryable *instance*, a query provider must be supplied during compilation.</param>
        public Func<TResult> Compile(IQueryProvider provider = null)
        {
            if (_compiled == null)
                compile(provider, null);
            return _compiled;
        }

        /// <summary>Executes the compiled query. The query will be compiled at this point if this hasn't been done yet.</summary>
        public TResult Invoke()
        {
            if (_compiled == null)
                compile(null, null);
            return _compiled();
        }
    }

    /// <summary>Encapsulates a compiled query taking one argument and returning a result.</summary>
    /// <typeparam name="T">Type of the argument taken by the query.</typeparam>
    /// <typeparam name="TResult">Type of the query result.</typeparam>
    public sealed class CompiledQuery<T, TResult> : CompiledQuery
    {
        private Expression<Func<T, TResult>> _query;
        private Func<T, TResult> _compiled;

        /// <summary>Constructor.</summary>
        /// <param name="query">The query to be compiled, as an expression.</param>
        public CompiledQuery(Expression<Func<T, TResult>> query)
        {
            if (query == null) throw new ArgumentNullException();
            _query = query;
        }

        private void compile(IQueryProvider provider, object[] args)
        {
            var compiled = CompiledQuery.Compile(_query, provider, args);
            System.Threading.Interlocked.CompareExchange(ref _compiled, (Func<T, TResult>) compiled, null);
        }

        /// <summary>Compiles the query (unless it's already been compiled earlier). Returns a delegate that represents the compiled query.</summary>
        /// <param name="provider">In cases where the query expression itself doesn't reference a queryable *instance*, a query provider must be supplied during compilation.</param>
        public Func<T, TResult> Compile(IQueryProvider provider = null)
        {
            if (_compiled == null)
                compile(provider, null);
            return _compiled;
        }

        /// <summary>Executes the compiled query. The query will be compiled at this point if this hasn't been done yet.</summary>
        public TResult Invoke(T arg)
        {
            if (_compiled == null)
                compile(null, null);
            return _compiled(arg);
        }
    }

}