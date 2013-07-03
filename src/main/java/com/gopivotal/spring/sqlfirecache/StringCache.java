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
import java.sql.Types;

import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

/**
 * A String cache that simply stores and retrieves strings.
 *
 * @author cdelashmutt
 */
public class StringCache
	extends AbstractSQLFireCache
{
	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getCreateColumns()
	 */
	@Override
	protected String getCreateColumns()
	{
		// TODO Auto-generated method stub
		return "(ID VARCHAR(1024), DATA LONG VARCHAR, PRIMARY KEY(ID)) PARTITION BY PRIMARY KEY";
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getIdType()
	 */
	@Override
	protected int getIdType()
	{
		// TODO Auto-generated method stub
		return Types.VARCHAR;
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getColumnsForGet()
	 */
	@Override
	protected String getColumnsForGet()
	{
		// TODO Auto-generated method stub
		return "DATA";
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getRowMapper()
	 */
	@Override
	protected RowMapper<String> getRowMapper()
	{
		return new RowMapper<String>()
		{
			@Override
			public String mapRow(ResultSet rs, int rowNum)
				throws SQLException
			{
				return rs.getString("DATA");
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getInsertColumns()
	 */
	@Override
	protected String getInsertColumns()
	{
		return "ID, DATA";
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

}
