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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

/**
 * A String cache that simply stores and retrieves strings.
 *
 * @author cdelashmutt
 */
public class StringCache
	extends AbstractColumnDefinedSQLFireCache
{
	
	final List<ColumnDefinition> idColumns = Arrays.asList(new ColumnDefinition("ID", SQLFType.VARCHAR, 1024));

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getIdColumns()
	 */
	@Override
	protected List<ColumnDefinition> getIdColumns()
	{
		// TODO Auto-generated method stub
		return idColumns;
	}

	final List<ColumnDefinition> dataColumns = Arrays.asList(new ColumnDefinition("DATA", SQLFType.LONGVARCHAR));
	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getDataColumns()
	 */
	@Override
	protected List<ColumnDefinition> getDataColumns()
	{
		return dataColumns;
	}
	
	private PreparedStatementSetter getIdPreparedStatementSetter(final String key)
	{
		return new PreparedStatementSetter()
		{
			@Override
			public void setValues(PreparedStatement ps)
				throws SQLException
			{
				ps.setString(1, key);
			}
		};

	}
	
	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getSelectPreparedStatementSetter(java.lang.Object)
	 */
	@Override
	protected PreparedStatementSetter getSelectPreparedStatementSetter(
			Object key)
	{
		// This will be a string since our ID is a VARCHAR
		return getIdPreparedStatementSetter((String)key);
	}

	final RowMapper<String> rowMapper = new RowMapper<String>()
	{
		@Override
		public String mapRow(ResultSet rs, int rowNum)
			throws SQLException
		{
			return rs.getString("DATA");
		}
	};
	
	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getRowMapper()
	 */
	@Override
	protected RowMapper<String> getRowMapper()
	{
		return rowMapper;
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getInsertPreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected PreparedStatementSetter getInsertPreparedStatementSetter(
			Object key, Object value)
	{
		final String stringKey = (String) key;
		final String stringValue = (String) value;

		return new PreparedStatementSetter()
		{
			@Override
			public void setValues(PreparedStatement ps)
				throws SQLException
			{
				ps.setString(1, stringKey);
				ps.setString(2, stringValue);
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getDeletePreparedStatementSetter(java.lang.Object)
	 */
	@Override
	protected PreparedStatementSetter getDeletePreparedStatementSetter(
			Object key)
	{
		// This will be a string since our ID is a VARCHAR
		return getIdPreparedStatementSetter((String)key);
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getUpdatePreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected PreparedStatementSetter getUpdatePreparedStatementSetter(
			Object key, Object value)
	{
		final String stringKey = (String) key;
		final String stringValue = (String) value;

		return new PreparedStatementSetter()
		{
			@Override
			public void setValues(PreparedStatement ps)
				throws SQLException
			{
				//String and key are reversed for a update statement
				ps.setString(1, stringValue);
				ps.setString(2, stringKey);
			}
		};
	}
}
