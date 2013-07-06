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

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Enumeration of SQLFire Types
 * 
 * @author cdelashmutt
 */
public enum SQLFType
{
	BIGINT(Long.class), BLOB(Blob.class), CHAR(String.class), BINARY(
			byte[].class, "CHAR"), CLOB(Clob.class), DATE(Date.class), DECIMAL(
			BigDecimal.class), DOUBLE(Double.class), FLOAT(Double.class),
	INTEGER(Integer.class), LONGVARCHAR(String.class, "LONG VARCHAR"),
	LONGVARBINARY(byte[].class, "LONG VARCHAR FOR BIT DATA"), NUMERIC(
			BigDecimal.class), REAL(Float.class), SMALLINT(Short.class), TIME(
			Time.class), TIMESTAMP(Timestamp.class), VARCHAR(String.class),
	VARBINARY(byte[].class, "VARCHAR");

	private String sqlName;

	private Class<?> javaType;

	SQLFType()
	{
	}

	SQLFType(Class<?> javaType)
	{
		this.javaType = javaType;
	}

	SQLFType(Class<?> javaType, String sqlName)
	{
		this(javaType);
		this.sqlName = sqlName;
	}

	public String getSQLName()
	{
		if (sqlName == null)
			return name();
		else
			return sqlName;
	}

	/**
	 * @return The Java type used for this SQLFType.
	 */
	public Class<?> getJavaType()
	{
		return javaType;
	}
}
