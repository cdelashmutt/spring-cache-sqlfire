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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
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

	/**
	 * TODO: Describe valuePrepend
	 */
	private String valuePrepend = "v_";

	/**
	 * @return the valuePrepend
	 */
	public String getValuePrepend()
	{
		return valuePrepend;
	}

	/**
	 * @param valuePrepend the valuePrepend to set
	 */
	public void setValuePrepend(String valuePrepend)
	{
		this.valuePrepend = valuePrepend;
	}

	/**
	 * TODO: Describe keyPrepend
	 */
	private String keyPrepend = "k_";

	
	/**
	 * @return the keyPrepend
	 */
	public String getKeyPrepend()
	{
		return keyPrepend;
	}

	/**
	 * @param keyPrepend the keyPrepend to set
	 */
	public void setKeyPrepend(String keyPrepend)
	{
		this.keyPrepend = keyPrepend;
	}

	private Function<ColumnDefinition, String> idNameFunction = new Function<ColumnDefinition, String>()
	{
		public String apply(ColumnDefinition input)
		{
			return keyPrepend + input.getName();
		}
	};

	private Function<ColumnDefinition, String> valueNameFunction = new Function<ColumnDefinition, String>()
	{
		public String apply(ColumnDefinition input)
		{
			return valuePrepend + input.getName();
		}
	};

	/**
	 * Creates a Create SQL statement fragment for the specified column
	 * definitions.
	 * 
	 * @param The string to prepend to the column name.
	 * @param columnDefs
	 *            The column definitions to build a statement for.
	 * @return The SQL fragment for the specified columns.
	 */
	private String buildCreateColumnsFragment(final String prepend, List<ColumnDefinition> columnDefs)
	{
		return StringUtils.collectionToDelimitedString(Lists.transform(
				columnDefs, new Function<ColumnDefinition, String>()
				{
					public String apply(ColumnDefinition input)
					{
						return prepend + input.getName() + " " + input.buildColumnTypeDefinitionSQL();
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
						Lists.transform(idColumns, idNameFunction), ", ") + ")";
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
				+ buildCreateColumnsFragment(keyPrepend, getIdColumns()) + ", "
				+ buildCreateColumnsFragment(valuePrepend, getDataColumns()) + ", "
				+ buildPrimaryKeyClause(getIdColumns()) + ")"
				+ " PARTITION BY PRIMARY KEY";
	}

	/**
	 * Returns the column definitions for the data columns
	 * 
	 * @return The ordered list of Data columns
	 */
	protected abstract List<ColumnDefinition> getDataColumns();

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getDeletePreparedStatementSetter(java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getDeletePreparedStatementSetter(
			final Object key)
	{
		return getIdParameterSource(key);
	}

	/**
	 * Provides a default strategy for producing a primary key based parameter source.
	 * 
	 * Single valued keys are simply returned as the value for the single id column.
	 * 
	 * Multi-valued keys are mapped from the property names of the passed object.
	 *
	 * @param key The key value object to use.
	 * @return The parameter source
	 */
	protected SqlParameterSource getIdParameterSource(final Object key)
	{
		if(getIdColumns().size() == 1)
		{
			return new MapSqlParameterSource(keyPrepend + getIdColumns().get(0).getName(), key);
		}
		else
		{
			return new PrependedBeanPropertySqlParameterSource(key, keyPrepend);
		}
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getDeleteSQL()
	 */
	@Override
	protected String getDeleteSQL()
	{
		return "DELETE FROM " + getFQTableName();
	}

	private Function<ColumnDefinition, String> idNameAndPlaceholderFunction = new Function<ColumnDefinition, String>()
	{
		@Override
		public String apply(ColumnDefinition input)
		{
			return keyPrepend + input.getName() + "=" + ":" + keyPrepend + input.getName();
		}
	};

	private Function<ColumnDefinition, String> valueNameAndPlaceholderFunction = new Function<ColumnDefinition, String>()
	{
		@Override
		public String apply(ColumnDefinition input)
		{
			return valuePrepend + input.getName() + "=" + ":" + valuePrepend + input.getName();
		}
	};

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getDeleteWhereClause()
	 */
	@Override
	protected String getDeleteWhereClause()
	{
		return "WHERE "
				+ StringUtils.collectionToDelimitedString(Lists.transform(
						getIdColumns(), idNameAndPlaceholderFunction), ", ");
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

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getInsertPreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getInsertPreparedStatementSetter(
			final Object key, final Object value)
	{
		return new PrioritySqlParameterSource(
				getPrependedSqlParameterSource(keyPrepend, getIdColumns(), key)
				, getPrependedSqlParameterSource(valuePrepend, getDataColumns(), value));
	}

	private SqlParameterSource getPrependedSqlParameterSource(final String prepend, final List<ColumnDefinition> columns, final Object obj)
	{
		if(columns.size() == 1)
		{
			return new MapSqlParameterSource(prepend + columns.get(0).getName(), obj);
		}
		else
		{
			return new PrependedBeanPropertySqlParameterSource(obj, prepend);
		}
	}

	private Function<ColumnDefinition, String> idPlaceHolderFunction = new Function<ColumnDefinition, String>()
	{
		public String apply(ColumnDefinition input)
		{
			return ":" + keyPrepend + input.getName();
		}
	};

	private Function<ColumnDefinition, String> valuePlaceHolderFunction = new Function<ColumnDefinition, String>()
	{
		public String apply(ColumnDefinition input)
		{
			return ":" + valuePrepend + input.getName();
		}
	};

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getInsertSQL()
	 */
	@Override
	protected String getInsertSQL()
	{
		String insertSQL = "insert into "
				+ getFQTableName()
				+ "("
				+ StringUtils.collectionToDelimitedString(
						Lists.transform(getIdColumns(), idNameFunction), ", ")
				+ ", "
				+ StringUtils.collectionToDelimitedString(
						Lists.transform(getDataColumns(), valueNameFunction), ", ")
				+ ") VALUES ("
				+ StringUtils.arrayToDelimitedString(Iterables.toArray(
						Iterables.concat(Iterables.transform(getIdColumns(), idPlaceHolderFunction),
								Iterables.transform(getDataColumns(), valuePlaceHolderFunction)),
						String.class), ", ") + ")";
		return insertSQL;
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getRowMapper()
	 */
	@Override
	protected RowMapper<?> getRowMapper()
	{
		if(getDataColumns().size() == 1)
		{
			return getSingleColumnRowMapper(getDataColumns().get(0).getType().getJavaType());
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

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getSelectSQL()
	 */
	@Override
	protected String getSelectSQL()
	{
		return "SELECT "
				+ StringUtils.collectionToDelimitedString(
						Lists.transform(getDataColumns(), valueNameFunction), ", ")
				+ " FROM "
				+ getFQTableName()
				+ " WHERE "
				+ StringUtils.collectionToDelimitedString(Lists.transform(
						getIdColumns(), idNameAndPlaceholderFunction), ", ");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#
	 * getUpdatePreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getUpdatePreparedStatementSetter(
			Object key, Object value)
	{
		return new PrioritySqlParameterSource(
				getPrependedSqlParameterSource(keyPrepend, getIdColumns(), key)
				, getPrependedSqlParameterSource(valuePrepend, getDataColumns(), value));		
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getUpdateSQL()
	 */
	@Override
	protected String getUpdateSQL()
	{
		return "UPDATE "
				+ getFQTableName()
				+ " SET "
				+ StringUtils.collectionToDelimitedString(Lists.transform(
						getDataColumns(), valueNameAndPlaceholderFunction), ", ")
				+ " WHERE "
				+ StringUtils.collectionToDelimitedString(Lists.transform(
						getIdColumns(), idNameAndPlaceholderFunction), ", ");
	}

}