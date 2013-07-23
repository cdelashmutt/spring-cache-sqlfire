/*
 * Licensed to the Apache Software Foundation (ASF) under one 
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gopivotal.spring.sqlfirecache;

import java.util.List;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Abstract base for an SQLFire based Cache implementation for Spring's cache
 * abstraction using columns as the configurable element, rather than having to
 * write SQL strings.
 * 
 * The requisite SQL is generated from the column definitions. Columns are
 * placed into the SQL statements in the order they are defined in the given
 * list.
 * 
 * @author cdelashmutt
 */
public abstract class AbstractColumnDefinedSQLFireCache
	extends AbstractSQLFireCache
{

	private ColumnDefinition idColumn = new ColumnDefinition("k_ID",
			SQLFType.INTEGER);
	
	/**
	 * @return the idColumn
	 */
	public ColumnDefinition getIdColumn()
	{
		return idColumn;
	}

	private Function<ColumnDefinition, String> valueNameFunction = new Function<ColumnDefinition, String>()
	{
		public String apply(ColumnDefinition input)
		{
			return input.getName();
		}
	};

	/**
	 * Creates a Create SQL statement fragment for the specified column
	 * definitions.
	 * 
	 * @param The
	 *            string to prepend to the column name.
	 * @param columnDefs
	 *            The column definitions to build a statement for.
	 * @return The SQL fragment for the specified columns.
	 */
	private String buildCreateColumnsFragment(List<ColumnDefinition> columnDefs)
	{
		return StringUtils.collectionToDelimitedString(Lists.transform(
				columnDefs, new Function<ColumnDefinition, String>()
				{
					public String apply(ColumnDefinition input)
					{
						return input.getName() + " "
								+ input.buildColumnTypeDefinitionSQL();
					}
				}), ", ");
	}

	/**
	 * Creates the primary key clause for the create statement
	 * 
	 * @param idColumns
	 *            The ordered collection of ID columns to use in the primary key
	 *            statement.
	 * @return The SQL fragement for the primary key clause.
	 */
	private String buildPrimaryKeyClause(ColumnDefinition idColumns)
	{
		return "PRIMARY KEY(" + idColumn.getName() + ")";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getCreateSQL()
	 */
	@Override
	protected String getCreateSQL()
	{
		return "CREATE TABLE " + getFQTableName() + " (" + idColumn.getName()
				+ " " + idColumn.buildColumnTypeDefinitionSQL() + ", "
				+ buildCreateColumnsFragment(getDataColumns()) + ", "
				+ buildPrimaryKeyClause(idColumn) + ")"
				+ " PARTITION BY PRIMARY KEY";
	}

	/**
	 * Returns the column definitions for the data columns
	 * 
	 * @return The ordered list of Data columns
	 */
	protected abstract List<ColumnDefinition> getDataColumns();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#
	 * getDeletePreparedStatementSetter(java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getDeletePreparedStatementSetter(
			final Object key)
	{
		return getIdParameterSource(key);
	}

	/**
	 * Provides a default strategy for producing a primary key based parameter
	 * source.
	 * 
	 * Single valued keys are simply returned as the value for the single id
	 * column.
	 * 
	 * Multi-valued keys are mapped from the property names of the passed
	 * object.
	 * 
	 * @param key
	 *            The key value object to use.
	 * @return The parameter source
	 */
	protected SqlParameterSource getIdParameterSource(final Object key)
	{
		return new MapSqlParameterSource(idColumn.getName(), key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getDeleteSQL()
	 */
	@Override
	protected String getDeleteSQL()
	{
		return "DELETE FROM " + getFQTableName();
	}

	private Function<ColumnDefinition, String> nameAndPlaceholderFunction = new Function<ColumnDefinition, String>()
	{
		@Override
		public String apply(ColumnDefinition input)
		{
			return input.getName() + "=" + ":" + input.getName();
		}
	};

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getDeleteWhereClause
	 * ()
	 */
	@Override
	protected String getDeleteWhereClause()
	{
		return "WHERE " + nameAndPlaceholderFunction.apply(idColumn);
	}

	/**
	 * Gets the schema qualified table name for this cache.
	 * 
	 * @return The fully qualified table name
	 */
	protected String getFQTableName()
	{
		return getSchemaName() + "." + getName();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#
	 * getInsertPreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getInsertPreparedStatementSetter(
			final Object key, final Object value)
	{
		return new PrioritySqlParameterSource(new MapSqlParameterSource(
				idColumn.getName(), key), new BeanPropertySqlParameterSource(
				value));
	}

	private Function<ColumnDefinition, String> placeHolderFunction = new Function<ColumnDefinition, String>()
	{
		public String apply(ColumnDefinition input)
		{
			return ":" + input.getName();
		}
	};

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getInsertSQL()
	 */
	@Override
	protected String getInsertSQL()
	{
		String insertSQL = "insert into "
				+ getFQTableName()
				+ "("
				+ idColumn.getName()
				+ ", "
				+ StringUtils.collectionToDelimitedString(
						Lists.transform(getDataColumns(), valueNameFunction),
						", ")
				+ ") VALUES ("
				+ placeHolderFunction.apply(idColumn)
				+ ", "
				+ StringUtils.collectionToDelimitedString(
						Lists.transform(getDataColumns(), placeHolderFunction),
						", ") + ")";
		return insertSQL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getRowMapper()
	 */
	@Override
	protected RowMapper<?> getRowMapper()
	{
		if (getDataColumns().size() == 1)
		{
			return getSingleColumnRowMapper(getDataColumns().get(0).getType()
					.getJavaType());
		}
		else
		{
			return BeanPropertyRowMapper.newInstance(Object.class);
		}
	}

	private <T> SingleColumnRowMapper<T> getSingleColumnRowMapper(Class<T> type)
	{
		return new SingleColumnRowMapper<T>(type);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#
	 * getSelectPreparedStatementSetter(java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getSelectPreparedStatementSetter(
			final Object key)
	{
		return getIdParameterSource(key);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getSelectSQL()
	 */
	@Override
	protected String getSelectSQL()
	{
		return "SELECT "
				+ StringUtils.collectionToDelimitedString(
						Lists.transform(getDataColumns(), valueNameFunction),
						", ")
				+ " FROM "
				+ getFQTableName()
				+ " WHERE "
				+ nameAndPlaceholderFunction.apply(idColumn);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#
	 * getUpdatePreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getUpdatePreparedStatementSetter(Object key,
			Object value)
	{
		return new PrioritySqlParameterSource(new MapSqlParameterSource(
				idColumn.getName(), key), new BeanPropertySqlParameterSource(
				value));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getUpdateSQL()
	 */
	@Override
	protected String getUpdateSQL()
	{
		return "UPDATE "
				+ getFQTableName()
				+ " SET "
				+ StringUtils.collectionToDelimitedString(Lists.transform(
						getDataColumns(), nameAndPlaceholderFunction),
						", ")
				+ " WHERE "
				+ nameAndPlaceholderFunction.apply(idColumn);
	}

}