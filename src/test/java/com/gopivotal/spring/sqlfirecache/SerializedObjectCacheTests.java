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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.sql.DataSource;

import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.auto.Auto;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for SerializedObjectCache
 *
 * @author cdelashmutt
 */
public class SerializedObjectCacheTests
{

	@Rule
	public final JUnitRuleMockery context = new JUnitRuleMockery();
	@Mock
	private DataSource dataSource;
	@Mock
	private Connection con;
	@Mock
	private Statement stm;
	@Mock
	private DatabaseMetaData dbmd;
	@Mock
	private ResultSet schemaRS;
	@Auto
	private Sequence schemaRSSeq;
	@Mock
	private ResultSet tableRS;
	@Auto
	private Sequence tableRSSeq;
	
	@Test
	public void testCreateSchemaAndTable()
	throws Exception
	{
		context.checking(new Expectations() {{
			oneOf(dataSource).getConnection(); will(returnValue(con));
			oneOf(con).createStatement(); will(returnValue(stm));
			oneOf(stm).execute(with(equal("create schema " + SerializedObjectCache.SCHEMA_NAME)));
			oneOf(stm).execute(with(equal("create table " + SerializedObjectCache.SCHEMA_NAME + ".books ( ID INT NOT NULL, OBJECT BLOB, PRIMARY KEY (ID)) partition by primary key")));
			allowing(con);
			allowing(stm);
		}});
		SerializedObjectCache cache = new SerializedObjectCache();
		cache.setName("books");
		cache.setDataSource(dataSource);
		cache.afterPropertiesSet();
	}

	@Test
	public void testCreateTable()
	throws Exception
	{
		context.checking(new Expectations() {{
			oneOf(dataSource).getConnection(); will(returnValue(con));
			oneOf(con).createStatement(); will(returnValue(stm));
			oneOf(con).getMetaData(); will(returnValue(dbmd));
			oneOf(dbmd).getSchemas(null, SerializedObjectCache.SCHEMA_NAME); will(returnValue(schemaRS));
			oneOf(schemaRS).next(); inSequence(schemaRSSeq); will(returnValue(true));
			oneOf(schemaRS).getString("TABLE_SCHEM"); inSequence(schemaRSSeq); will(returnValue(SerializedObjectCache.SCHEMA_NAME));
			oneOf(schemaRS).close(); inSequence(schemaRSSeq);
			oneOf(dbmd).getTables(null, SerializedObjectCache.SCHEMA_NAME, "books", null);
			oneOf(stm).execute("create table " + SerializedObjectCache.SCHEMA_NAME + ".books ( ID INT NOT NULL, OBJECT BLOB, PRIMARY KEY (ID)) partition by primary key");
			oneOf(stm).close();
			oneOf(con).close();
		}});
		SerializedObjectCache cache = new SerializedObjectCache();
		cache.setName("books");
		cache.setDataSource(dataSource);
		cache.afterPropertiesSet();
	}

	@Test
	public void testNoCreate()
	throws Exception
	{
		context.checking(new Expectations() {{
			oneOf(dataSource).getConnection(); will(returnValue(con));
			oneOf(con).getMetaData(); will(returnValue(dbmd));
			oneOf(dbmd).getSchemas(null, SerializedObjectCache.SCHEMA_NAME); will(returnValue(schemaRS));
			oneOf(schemaRS).next(); inSequence(schemaRSSeq); will(returnValue(true));
			oneOf(schemaRS).getString("TABLE_SCHEM"); inSequence(schemaRSSeq); will(returnValue(SerializedObjectCache.SCHEMA_NAME));
			oneOf(schemaRS).close(); inSequence(schemaRSSeq);
			oneOf(dbmd).getTables(null, SerializedObjectCache.SCHEMA_NAME, "books", null); will(returnValue(tableRS));
			oneOf(tableRS).next(); inSequence(tableRSSeq); will(returnValue(true));
			oneOf(tableRS).getString("TABLE_NAME"); inSequence(tableRSSeq); will(returnValue("books"));
			oneOf(tableRS).close(); inSequence(tableRSSeq);
			oneOf(con).close();
		}});
		SerializedObjectCache cache = new SerializedObjectCache();
		cache.setName("books");
		cache.setDataSource(dataSource);
		cache.afterPropertiesSet();
	}
}