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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.support.SqlLobValue;

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
	extends AbstractColumnDefinedSQLFireCache
{
	private Logger log = LoggerFactory.getLogger(SerializedObjectCache.class);

	final ColumnDefinition idColumn = new ColumnDefinition("ID", SQLFType.INTEGER);
	final List<ColumnDefinition> idColumns = Arrays.asList(idColumn);

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getIdColumns()
	 */
	@Override
	protected List<ColumnDefinition> getIdColumns()
	{
		// TODO Auto-generated method stub
		return idColumns;
	}

	final ColumnDefinition dataColumn = new ColumnDefinition("OBJECT", SQLFType.BLOB);
	final List<ColumnDefinition> dataColumns = Arrays.asList(dataColumn);
	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getDataColumns()
	 */
	@Override
	protected List<ColumnDefinition> getDataColumns()
	{
		return dataColumns;
	}

	final RowMapper<Object> rowMapper = new RowMapper<Object>()
			{
		@Override
		public Object mapRow(ResultSet rs, int rowNum)
			throws SQLException
		{
			Blob blob = rs.getBlob(getValuePrepend() + "OBJECT");
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

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractSQLFireCache#getRowMapper()
	 */
	@Override
	protected RowMapper<Object> getRowMapper()
	{
		return rowMapper;
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getInsertPreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	protected SqlParameterSource getInsertPreparedStatementSetter(
			final Object key, final Object value)
	{
		return new IntBlobParameterSource(key, value);
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getUpdatePreparedStatementSetter(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected SqlParameterSource getUpdatePreparedStatementSetter(
			final Object key, final Object value)
	{
		return new IntBlobParameterSource(key, value);
	}

	private class IntBlobParameterSource
	implements SqlParameterSource
	{
		final Object key;
		final Object value;
		final String prependedIdColumn = getKeyPrepend() + idColumn.getName();
		final String prependedDataColumn = getValuePrepend() + dataColumn.getName();
		
		/**
		 * TODO: Describe IntBlobParameterSource constructor
		 *
		 */
		public IntBlobParameterSource(Object key, Object value)
		{
			super();
			this.key = key;
			this.value = value;
		}

		@Override
		public boolean hasValue(String paramName)
		{
			return prependedIdColumn.equals(paramName)
				|| prependedDataColumn.equals(paramName);
		}

		@Override
		public Object getValue(String paramName)
			throws IllegalArgumentException
		{
			if(prependedDataColumn.equals(paramName))
			{
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				try
				{
					ObjectOutputStream oos = new ObjectOutputStream(bos);
					oos.writeObject(value);
				}
				catch(IOException e)
				{
					throw new RuntimeException("Error serializing object to cache", e);
				}
				return new SqlLobValue(bos.toByteArray());
			}
			else if(prependedIdColumn.equals(paramName))
			{
				return key;
			}
			else
			{
				throw new IllegalArgumentException("No parameter with name " + paramName);
			}
		}

		@Override
		public int getSqlType(String paramName)
		{
			if(prependedDataColumn.equals(paramName))
				return Types.BLOB;
			else if(prependedIdColumn.equals(paramName))
			{
				return Types.INTEGER;
			}
			else
			{
				return TYPE_UNKNOWN;
			}
		}

		@Override
		public String getTypeName(String paramName)
		{
			if(prependedDataColumn.equals(paramName))
				return "BLOB";
			else if(prependedIdColumn.equals(paramName))
			{
				return "INTEGER";
			}
			else
			{
				return null;
			}
		}
	};

}
