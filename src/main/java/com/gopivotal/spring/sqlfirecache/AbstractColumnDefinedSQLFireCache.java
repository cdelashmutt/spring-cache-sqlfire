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

import java.util.Arrays;
import java.util.List;

import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
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

	private Function<ColumnDefinition,String> nameFunction = new Function<ColumnDefinition, String>()
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
	 * @param columnDefs
	 *            The column definitions to build a statement for.
	 * @return The SQL fragment for the specified columns.
	 */
	private String buildCreateColumnsFragment(List<ColumnDefinition> columnDefs)
	{
		return StringUtils.collectionToDelimitedString(
				Lists.transform(columnDefs, new Function<ColumnDefinition, String>()
				{
					public String apply(ColumnDefinition input)
					{
						return input.buildColumnDefinitionSQL();
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
	private String buildPrimaryKeyClause(List<ColumnDefinition> idColumns)
	{
		return "PRIMARY KEY("
				+ StringUtils.collectionToDelimitedString(
						Lists.transform(idColumns, nameFunction),
						", ") + ")";
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
		return "CREATE TABLE " + getFQTableName() + " ("
				+ buildCreateColumnsFragment(getIdColumns()) + ", "
				+ buildCreateColumnsFragment(getDataColumns()) + ", "
				+ buildPrimaryKeyClause(getIdColumns()) + ")"
				+ " PARTITION BY PRIMARY KEY";
	}

	/**
	 * Returns the column definitions for the data columns
	 * 
	 * @return The ordered list of Data columns
	 */
	protected abstract List<ColumnDefinition> getDataColumns();

	/**
	 * Sets the appropriate parameters for the delete statement
	 * 
	 * @return A parameter setter.
	 */
	@Override
	protected abstract PreparedStatementSetter getDeletePreparedStatementSetter(
			final Object key);

	/**
	 * Provides the delete statement root necessary to evict individual entries,
	 * or clear the entire table.
	 * 
	 * @return The delete statement
	 */
	@Override
	protected String getDeleteSQL()
	{
		return "delete from " + getFQTableName();
	}

	private Function<ColumnDefinition, String> nameAndPlaceholderFunction = new Function<ColumnDefinition, String>()
	{
		@Override
		public String apply(ColumnDefinition input)
		{
			return ((ColumnDefinition) input).getName() + "=?";
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
		return "WHERE "
				+ StringUtils.collectionToDelimitedString(Lists.transform(getIdColumns(),
						nameAndPlaceholderFunction), ", ");
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

	/**
	 * Returns the column definitions for the Id columns
	 * 
	 * @return The ordered list of Id columns
	 */
	protected abstract List<ColumnDefinition> getIdColumns();

	/**
	 * Provides a PreparedStatementSetter used to put values into an insert
	 * statement
	 * 
	 * @param key
	 *            The key to use for an insert into SQLFire
	 * @param value
	 *            The value to store in SQLFire
	 * @return The PreparedStatementSetter that can properly set up the
	 */
	@Override
	protected abstract PreparedStatementSetter getInsertPreparedStatementSetter(
			final Object key, final Object value);

	/**
	 * Builds the statement used to support the put cache operation.
	 * 
	 * This statement is typically an insert that fills all the columns of the
	 * table needed to store the cached object.
	 * 
	 * @return A statement used to put objects into the cache.
	 */
	@Override
	protected String getInsertSQL()
	{
		String insertSQL = "insert into "
				+ getFQTableName()
				+ "("
				+ StringUtils.collectionToDelimitedString(Lists.transform(getIdColumns(),
						nameFunction), ", ")
				+ ", "
				+ StringUtils.collectionToDelimitedString(Lists.transform(getDataColumns(),
						nameFunction), ", ")
				+ ") VALUES (";
		Character placeHolders[] = new Character[getIdColumns().size() + getDataColumns().size()];
		Arrays.fill(placeHolders, '?');
		insertSQL += StringUtils.arrayToCommaDelimitedString(placeHolders) + ")";
		return insertSQL;
	}

	/**
	 * Maps returned rows from SQLFire to objects.
	 * 
	 * @return A RowMapper
	 */
	@Override
	protected abstract RowMapper<?> getRowMapper();

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#
	 * getSelectPreparedStatementSetter(java.lang.Object)
	 */
	@Override
	protected abstract PreparedStatementSetter getSelectPreparedStatementSetter(
			final Object key);

	/**
	 * Returns the query string to support the get operation for the cache.
	 * 
	 * Typically the query will return the columns needed to reconstruct the
	 * object from the cached representation.
	 * 
	 * @return A string representing the query for looking up a single entry in
	 *         the cache table.
	 */
	@Override
	protected String getSelectSQL()
	{
		return "SELECT "
				+ StringUtils.collectionToDelimitedString(Lists.transform(getDataColumns(),
						nameFunction), ", ")
				+ " FROM "
				+ getFQTableName()
				+ " WHERE "
				+ StringUtils.collectionToDelimitedString(Lists.transform(getIdColumns(),
						nameAndPlaceholderFunction), ", ");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#
	 * getUpdatePreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected abstract PreparedStatementSetter getUpdatePreparedStatementSetter(
			Object key, Object value);

	/**
	 * Gets the update statement for this cache table.
	 * 
	 * @return The SQL statement to update the data in the cache table
	 */
	@Override
	protected String getUpdateSQL()
	{
		return "UPDATE "
				+ getFQTableName()
				+ " SET "
				+ StringUtils.collectionToDelimitedString(Lists.transform(getDataColumns(),
						nameAndPlaceholderFunction), ", ")
				+ " WHERE "
				+ StringUtils.collectionToDelimitedString(Lists.transform(getIdColumns(),
						nameAndPlaceholderFunction), ", ");
	}

}