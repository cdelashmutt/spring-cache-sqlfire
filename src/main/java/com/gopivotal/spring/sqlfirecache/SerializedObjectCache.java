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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

/**
 * A simple SQLFire cache definition that serializes/de-serializes objects into
 * a single BLOB column.
 * 
 * This cache will check for the existence of an appropriate table at start up.
 * If no table exists, then it will be created.
 * 
 * @author cdelashmutt
 */
public class SerializedObjectCache
	extends AbstractSQLFireCache
{
	private Logger log = LoggerFactory.getLogger(SerializedObjectCache.class);

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getCreateColumns()
	 */
	protected String getCreateColumns()
	{
		return "( ID INT NOT NULL, OBJECT BLOB, PRIMARY KEY (ID)) partition by primary key";
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getColumnsForGet()
	 */
	protected String getColumnsForGet()
	{
		// TODO Auto-generated method stub
		return "OBJECT";
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getRowMapper()
	 */
	protected RowMapper<Object> getRowMapper()
	{
		return new RowMapper<Object>()
		{
			@Override
			public Object mapRow(ResultSet rs, int rowNum)
				throws SQLException
			{
				Blob blob = rs.getBlob("OBJECT");
				ObjectInputStream ois = null;
				Object value = null;
				try
				{
					ois = new ObjectInputStream(blob.getBinaryStream());
					value = ois.readObject();
				}
				catch (Exception e)
				{
					throw new RuntimeException("Error de-serializing object", e);
				}
				finally
				{
					try
					{
						if (ois != null)
							ois.close();
					}
					catch (IOException e)
					{
						// Can't do anything on a close error
						log.warn("Error while closing input stream to Blob", e);
					}
				}
				return value;
			}

		};
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getInsertPreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	protected PreparedStatementSetter getInsertPreparedStatementSetter(
			final Object key, final Object value)
	{
		final int intKey = (Integer) key;

		return new PreparedStatementSetter()
		{
			@Override
			public void setValues(PreparedStatement ps)
				throws SQLException
			{
				ps.setInt(1, intKey);
				Blob blob = ps.getConnection().createBlob();
				ObjectOutputStream oos = null;
				try
				{
					oos = new ObjectOutputStream(blob.setBinaryStream(1));
					oos.writeObject(value);
					ps.setBlob(2, blob);
				}
				catch (IOException e)
				{
					throw new RuntimeException(
							"Error serializing object to blob store: ", e);
				}
				finally
				{
					try
					{
						if (oos != null)
							oos.close();
					}
					catch (IOException e)
					{
						log.warn(
								"Exception while closing Blob output stream: ",
								e);
					}
				}
			}
		};
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getInsertColumns()
	 */
	protected String getInsertColumns()
	{
		return "ID,OBJECT";
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getIdType()
	 */
	protected int getIdType()
	{
		return Types.INTEGER;
	}

}
