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

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

/**
 * Abstract base for an SQLFire based Cache implementation for Spring's cache
 * abstraction
 * 
 * @author cdelashmutt
 */
public abstract class AbstractSQLFireCache
	implements InitializingBean, Cache
{

	public static final String SCHEMA_NAME = "SPRINGCACHE";

	private DataSource dataSource;

	private String name;

	private JdbcTemplate template;

	private Logger log = LoggerFactory.getLogger(AbstractSQLFireCache.class);

	/**
	 * @param dataSource
	 *            the dataSource to set
	 */
	public void setDataSource(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet()
		throws Exception
	{
		template = new JdbcTemplate(dataSource);
		template.execute(new ConnectionCallback<Object>()
		{
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
									+ SCHEMA_NAME + "'");
					boolean foundSchema = false;
					try
					{
						while (schemas.next())
						{
							// We only get here if we found the schema.
							log.trace("Found schema: " + SCHEMA_NAME);
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
						log.debug("Creating schema: " + SCHEMA_NAME);
						stm.execute("create schema " + SCHEMA_NAME);
						createTable(stm);
					}
					else
					{
						ResultSet tables = stm
								.executeQuery("select * from SYS.SYSTABLES where TABLESCHEMANAME='"
										+ SCHEMA_NAME
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

			private void createTable(Statement stm)
				throws SQLException
			{
				log.debug("Creating table: " + SCHEMA_NAME + "." + getName());
				stm.execute("create table " + SCHEMA_NAME + "." + getName()
						+ " " + getCreateColumns());
			}
		});
	}

	/**
	 * Get's the create column string
	 * 
	 * @return The create column string
	 */
	protected abstract String getCreateColumns();

	/**
	 * @param name
	 *            The name to set for this cache
	 */
	public void setName(String name)
	{
		this.name = name;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cache.Cache#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cache.Cache#getNativeCache()
	 */
	@Override
	public Object getNativeCache()
	{
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.cache.Cache#get(java.lang.Object)
	 */
	@Override
	public ValueWrapper get(Object key)
	{
		try
		{
			Object returnVal = template.queryForObject("select "
					+ getColumnsForGet() + " from " + getFQTableName()
					+ " where ID=?", new Object[]
				{ key }, new int[]
				{ getIdType() }, getRowMapper());

			return new SimpleValueWrapper(returnVal);
		}
		catch (EmptyResultDataAccessException e)
		{
			// no data
			return null;
		}
	}

	/**
	 * Provides the SQL Type of the ID
	 * 
	 * @return The SQL type from java.sql.Types for the ID
	 */
	protected abstract int getIdType();

	/**
	 * The columns used for a query in a "get" operation.
	 * 
	 * These columns are used by the RowMapper provided by the getRowMapper
	 * method to construct an object.
	 * 
	 * @return The column names, separated by commas, needed for the RowMapper.
	 */
	protected abstract String getColumnsForGet();

	/**
	 * Maps returned rows from SQLFire to objects.
	 * 
	 * @return A RowMapper
	 */
	protected abstract RowMapper<?> getRowMapper();

	/* (non-Javadoc)
	 * @see org.springframework.cache.Cache#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void put(final Object key, final Object value)
	{
		template.update("insert into " + getFQTableName() + "("
				+ getInsertColumns() + ") VALUES (?,?)",
				getInsertPreparedStatementSetter(key, value));
	}

	/**
	 * Provide the column list used for inserting new items into the cache.
	 * 
	 * @return A comma separated string specifying the columns to use when
	 *         inserting records to SQLFire.
	 */
	protected abstract String getInsertColumns();

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
	protected abstract PreparedStatementSetter getInsertPreparedStatementSetter(
			final Object key, final Object value);

	@Override
	public void evict(Object key)
	{
		template.update(getDeleteSQL() + " WHERE ID=?", key);
	}

	/* (non-Javadoc)
	 * @see org.springframework.cache.Cache#clear()
	 */
	@Override
	public void clear()
	{
		template.execute(getDeleteSQL());
	}

	private String getDeleteSQL()
	{
		return "delete from " + getFQTableName();
	}

	private String getFQTableName()
	{
		return SCHEMA_NAME + "." + getName();
	}

}