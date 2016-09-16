using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace JosephGuadagno.AzureHelpers.Tests
{
	public static class ObjectHelper
	{
		public static bool AreEqual(object lhs, object rhs, params PropertyInfo[] excludedProperties)
		{
			return AreEqual(lhs, rhs, excludedProperties.AsEnumerable());
		}

		public static bool AreEqual(object lhs, object rhs, IEnumerable<PropertyInfo> excludedProperties)
		{
			if (lhs == null && rhs == null)
			{
				return true;
			}

			if (lhs == null || rhs == null)
			{
				return false;
			}

			if (lhs.GetType() != rhs.GetType())
			{
				return false;
			}

			if (IsPrimitivish(lhs.GetType()))
			{
				return Equals(lhs, rhs);
			}

			if (lhs.GetType().GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
			{
				return AreEqualCollections((IEnumerable)lhs, (IEnumerable)rhs, excludedProperties);
			}

			var propertyInfos = excludedProperties as PropertyInfo[] ?? excludedProperties.ToArray();
			return lhs.GetType().GetProperties().Except(propertyInfos).All(p => AreEqual(p.GetValue(lhs), p.GetValue(rhs), propertyInfos));
		}

		private static bool IsPrimitivish(Type type)
		{
			return IsPrimitivishNonNullable(type) || IsPrimitivishNullable(type);
		}

		private static bool IsPrimitivishNonNullable(Type type)
		{
			return
				type.IsPrimitive
				|| type == typeof(string)
				|| type == typeof(decimal)
				|| type == typeof(DateTime)
				|| type == typeof(Guid)
				|| type == typeof(DateTimeOffset);
		}

		private static bool IsPrimitivishNullable(Type type)
		{
			return
				type.IsGenericType
				&& type.GetGenericTypeDefinition() == typeof(Nullable<>)
				&& IsPrimitivishNonNullable(type.GetGenericArguments()[0]);
		}

		public static bool AreEqualCollections(IEnumerable lhs, IEnumerable rhs, params PropertyInfo[] excludedProperties)
		{
			return AreEqualCollections(lhs, rhs, excludedProperties.AsEnumerable());
		}

		public static bool AreEqualCollections(IEnumerable lhs, IEnumerable rhs, IEnumerable<PropertyInfo> excludedProperties)
		{
			var lhsList = lhs.OfType<object>().ToList();
			var rhsList = rhs.OfType<object>().ToList();

			if (lhsList.Count != rhsList.Count)
			{
				return false;
			}

			for (int i = 0; i < lhsList.Count; i++)
			{
				// ReSharper disable once PossibleMultipleEnumeration
				var propertyInfos = excludedProperties as PropertyInfo[] ?? excludedProperties.ToArray();
				if (!AreEqual(lhsList[i], rhsList[i], propertyInfos))
				{
					return false;
				}
			}

			return true;
		}
	}

	public static class CompareProperty
	{
		public static PropertyInfo Get<T>(Expression<Func<T, object>> propertyExpression)
		{
			var body = propertyExpression.Body;

			if (body.NodeType == ExpressionType.Convert)
			{
				var unaryExpression = body as UnaryExpression;
				if (unaryExpression == null)
				{
					throw new InvalidOperationException();
				}

				body = unaryExpression.Operand;
			}

			if (body.NodeType != ExpressionType.MemberAccess)
			{
				throw new InvalidOperationException();
			}

			var memberExpression = body as MemberExpression;
			if (memberExpression == null)
			{
				throw new InvalidOperationException();
			}

			var propertyInfo = memberExpression.Member as PropertyInfo;
			if (propertyInfo == null)
			{
				throw new InvalidOperationException();
			}

			return propertyInfo;
		}
	}
}