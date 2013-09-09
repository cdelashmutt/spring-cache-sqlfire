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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * A simple base class for caches that use SQL Statements to retrieve and store
 * data in SQLFire.
 * 
 * @author cdelashmutt
 */
public abstract class AbstractSQLFireCache
	implements InitializingBean, Cache
{

	private String schemaName = "SPRINGCACHE";

	private DataSource dataSource;

	private String name;

	private JdbcTemplate template;

	private NamedParameterJdbcTemplate namedTemplate;

	private Logger log = LoggerFactory
			.getLogger(AbstractColumnDefinedSQLFireCache.class);
	
	private boolean throwExceptions = false;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet()
		throws Exception
	{
		template = new JdbcTemplate(dataSource);
		namedTemplate = new NamedParameterJdbcTemplate(template);
		template.execute(new ConnectionCallback<Object>()
		{
			private void createTable(Statement stm)
				throws SQLException
			{
				log.debug("Creating table: " + schemaName + "." + getName());
				String createSQL = getCreateSQL();
				log.trace(createSQL);
				stm.execute(createSQL);
			}

			@Override
			public Object doInConnection(Connection con)
				throws SQLException, DataAccessException
			{
				Statement stm = null;
				try
				{
					stm = con.createStatement();
					ResultSet schemas = stm
							.executeQuery("select * from SYS.SYSSCHEMAS where SCHEMANAME='"
									+ schemaName + "'");
					boolean foundSchema = false;
					try
					{
						while (schemas.next())
						{
							// We only get here if we found the schema.
							log.trace("Found schema: " + schemaName);
							foundSchema = true;
							break;
						}
					}
					finally
					{
						if (schemas != null)
							schemas.close();
					}
					if (!foundSchema)
					{
						log.debug("Creating schema: " + schemaName);
						stm.execute("CREATE SCHEMA " + schemaName);
						createTable(stm);
					}
					else
					{
						ResultSet tables = stm
								.executeQuery("select * from SYS.SYSTABLES where TABLESCHEMANAME='"
										+ schemaName
										+ "' and TABLENAME='"
										+ getName().toUpperCase() + "'");
						boolean foundTable = false;
						try
						{
							while (tables.next())
							{
								// Only get here if we have the cache table
								log.trace("Found cache table: " + getName());
								foundTable = true;
								break;
							}
						}
						finally
						{
							if (tables != null)
								tables.close();
						}
						if (!foundTable)
						{
							createTable(stm);
						}
					}
				}
				finally
				{
					if (stm != null)
						stm.close();
				}
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cache.Cache#clear()
	 */
	@Override
	public void clear()
	{
		template.execute(getDeleteSQL());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cache.Cache#evict(java.lang.Object)
	 */
	@Override
	public void evict(Object key)
	{
		namedTemplate.update(getDeleteSQL() + " " + getDeleteWhereClause(),
				getDeletePreparedStatementSetter(key));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cache.Cache#get(java.lang.Object)
	 */
	@Override
	public ValueWrapper get(Object key)
	{
		try
		{
			List<?> results = namedTemplate.query(getSelectSQL(),
					getSelectPreparedStatementSetter(key), getRowMapper());

			if (results.size() == 0)
			{
				return null;
			}
			else if (results.size() > 1)
			{
				String message = "Multiple results returned for cache get select statement.  Check the validity "
						+ "of the create table statement and select statement to ensure that the id "
						+ "column(s) for the table are guarenteed to be unique.";
				if(throwExceptions)
				{
					throw new RuntimeException(message);
				}
				else
				{	
					log.warn(message);
					return null;
				}
			}
			else
			{
				return new SimpleValueWrapper(results.get(0));
			}
		}
		catch (DataAccessException e)
		{
			if(throwExceptions)
			{
				throw new RuntimeException(e);
			}
			else
			{
				log.warn("Error executing select statement for cache get", e);
				return null;
			}
		}
	}

	/**
	 * Returns the create SQL string used for creating the cache table, if
	 * needed.
	 * 
	 * @return The create SQL string.
	 */
	protected abstract String getCreateSQL();

	/**
	 * Provides a setter that can set any necessary parameters in the delete SQL
	 * String.
	 * 
	 * @param key
	 *            The key object used to identify a cached object.
	 * @return The setter that can set parameters on the prepared delete SQL
	 *         Statement.
	 */
	protected abstract SqlParameterSource getDeletePreparedStatementSetter(
			final Object key);

	/**
	 * Returns the delete SQL statement used to remove every cached object in
	 * the table.
	 * 
	 * @return The delete SQL string.
	 */
	protected abstract String getDeleteSQL();

	/**
	 * Returns a fragment WHERE clause used with the getDeleteSQL statement to
	 * remove a single cached object in the table.
	 * 
	 * The statement fragment should contain placeholders for the parameters
	 * that need to be passed in to the statement. The placeholders should be in
	 * the form of a named placeholder preceded by a colon, as in the same form
	 * used for named parameters in the NamedParameterJdbcTemplate.
	 * 
	 * @return The delete SQL WHERE clause fragment.
	 */
	protected abstract String getDeleteWhereClause();

	/**
	 * Provides a setter that can set any necessary parameters in the insert SQL
	 * String.
	 * 
	 * @param key
	 *            The key object used to lookup a cached object.
	 * @param value
	 *            The value object to store in the cache.
	 * @return The setter that can set parameters on the prepared insert SQL
	 *         Statement.
	 */
	protected abstract SqlParameterSource getInsertPreparedStatementSetter(
			final Object key, final Object value);

	/**
	 * Returns the insert SQL statement used to store cached objects.
	 * 
	 * The statement should contain placeholders for the parameters that need to
	 * be passed in to the statement. The placeholders should be in the form of
	 * a named placeholder preceded by a colon, as in the same form used for
	 * named parameters in the NamedParameterJdbcTemplate.
	 * 
	 * @return The insert SQL string.
	 */
	protected abstract String getInsertSQL();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cache.Cache#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cache.Cache#getNativeCache()
	 */
	@Override
	public Object getNativeCache()
	{
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Maps a returned record from for the execution of the select SQL statement
	 * to an object.
	 * 
	 * @return The row mapper for the select SQL statement.
	 */
	protected abstract RowMapper<?> getRowMapper();

	/**
	 * @return the schemaName
	 */
	public String getSchemaName()
	{
		return schemaName;
	}

	/**
	 * Provides a setter that can set any necessary parameters in the select SQL
	 * String.
	 * 
	 * @param key
	 *            The key object used to lookup a cached object.
	 * @return The setter that can set parameters on the prepared select SQL
	 *         Statement.
	 */
	protected abstract SqlParameterSource getSelectPreparedStatementSetter(
			final Object key);

	/**
	 * Returns the select SQL statement used to lookup cached objects.
	 * 
	 * The statement should contain placeholders for the parameters that need to
	 * be passed in to the statement. The placeholders should be in the form of
	 * a named placeholder preceded by a colon, as in the same form used for
	 * named parameters in the NamedParameterJdbcTemplate.
	 * 
	 * @return The select SQL string.
	 */
	protected abstract String getSelectSQL();

	/**
	 * Provides a setter that can set any necessary parameters in the update SQL
	 * String.
	 * 
	 * @param key
	 *            The key object used to identify a cached object.
	 * @param value
	 *            The value object to store in the cache.
	 * @return The setter that can set parameters on the prepared update SQL
	 *         Statement.
	 */
	protected abstract SqlParameterSource getUpdatePreparedStatementSetter(
			Object key, Object value);

	/**
	 * Returns the update SQL statement used to update cached objects.
	 * 
	 * The statement should contain placeholders for the parameters that need to
	 * be passed in to the statement. The placeholders should be in the form of
	 * a named placeholder preceded by a colon, as in the same form used for
	 * named parameters in the NamedParameterJdbcTemplate.
	 * 
	 * @return The update SQL string.
	 */
	protected abstract String getUpdateSQL();

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.cache.Cache#put(java.lang.Object,
	 * java.lang.Object)
	 */
	@Override
	public void put(final Object key, final Object value)
	{
		try
		{
			int updateCount = namedTemplate.update(getUpdateSQL(),
					getUpdatePreparedStatementSetter(key, value));
			if (updateCount == 0)
			{
				namedTemplate.update(getInsertSQL(),
						getInsertPreparedStatementSetter(key, value));
			}
		}
		catch (Exception e)
		{
			if(throwExceptions)
			{
				throw new RuntimeException(e);
			}
			else
			{
				// Problems putting data into cache shouldn't stop the method.
				log.warn(
						"Exception while attempting to update or insert to cache table.",
						e);
			}
		}
	}

	/**
	 * @param dataSource
	 *            the dataSource to set
	 */
	public void setDataSource(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	/**
	 * @param name
	 *            The name to set for this cache
	 */
	public void setName(String name)
	{
		this.name = name;
	}

	/**
	 * @param schemaName
	 *            the schemaName to set
	 */
	public void setSchemaName(String schemaName)
	{
		this.schemaName = schemaName;
	}

	/**
	 * @return the throwExceptions setting.
	 */
	public boolean isThrowExceptions()
	{
		return throwExceptions;
	}

	/**
	 * @param throwExceptions Sets whether exceptions are thrown by the cache, or simply logged as warnings.
	 */
	public void setThrowExceptions(boolean throwExceptions)
	{
		this.throwExceptions = throwExceptions;
	}
	
}