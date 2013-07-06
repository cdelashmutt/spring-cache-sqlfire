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
public class SerializedObjectCacheCreateTests
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
		final SerializedObjectCache cache = new SerializedObjectCache();
		context.checking(new Expectations() {{
			oneOf(dataSource).getConnection(); will(returnValue(con));
			oneOf(con).createStatement(); will(returnValue(stm));
			oneOf(stm).execute(with(equal("CREATE SCHEMA " + cache.getSchemaName())));
			oneOf(stm).execute(with(equal("CREATE TABLE " + cache.getSchemaName() + ".books (k_ID INTEGER, v_OBJECT BLOB, PRIMARY KEY(k_ID)) PARTITION BY PRIMARY KEY")));
			allowing(con);
			allowing(stm);
		}});
		cache.setName("books");
		cache.setDataSource(dataSource);
		cache.afterPropertiesSet();
	}

	@Test
	public void testCreateTable()
	throws Exception
	{
		final SerializedObjectCache cache = new SerializedObjectCache();
		cache.setName("books");
		context.checking(new Expectations() {{
			oneOf(dataSource).getConnection(); will(returnValue(con));
			oneOf(con).createStatement(); will(returnValue(stm));
			oneOf(stm).executeQuery("select * from SYS.SYSSCHEMAS where SCHEMANAME='"+cache.getSchemaName()+"'"); will(returnValue(schemaRS));
			oneOf(schemaRS).next(); inSequence(schemaRSSeq); will(returnValue(true));
			oneOf(schemaRS).close(); inSequence(schemaRSSeq);
			oneOf(stm).executeQuery("select * from SYS.SYSTABLES where TABLESCHEMANAME='"
					+ cache.getSchemaName()
					+ "' and TABLENAME='"
					+ cache.getName().toUpperCase() + "'"); will(returnValue(tableRS));
			oneOf(tableRS).next(); inSequence(tableRSSeq); will(returnValue(false));
			oneOf(tableRS).close(); inSequence(tableRSSeq);
			oneOf(stm).execute("CREATE TABLE " + cache.getSchemaName() + ".books (k_ID INTEGER, v_OBJECT BLOB, PRIMARY KEY(k_ID)) PARTITION BY PRIMARY KEY");
			oneOf(stm).close();
			oneOf(con).close();
		}});
		cache.setName("books");
		cache.setDataSource(dataSource);
		cache.afterPropertiesSet();
	}

	@Test
	public void testNoCreate()
	throws Exception
	{
		final SerializedObjectCache cache = new SerializedObjectCache();
		cache.setName("books");
		context.checking(new Expectations() {{
			oneOf(dataSource).getConnection(); will(returnValue(con));
			oneOf(con).createStatement(); will(returnValue(stm));
			oneOf(stm).executeQuery("select * from SYS.SYSSCHEMAS where SCHEMANAME='"+cache.getSchemaName()+"'"); will(returnValue(schemaRS));
			oneOf(schemaRS).next(); inSequence(schemaRSSeq); will(returnValue(true));
			oneOf(schemaRS).close(); inSequence(schemaRSSeq);
			oneOf(stm).executeQuery("select * from SYS.SYSTABLES where TABLESCHEMANAME='"
					+ cache.getSchemaName()
					+ "' and TABLENAME='"
					+ cache.getName().toUpperCase() + "'"); will(returnValue(tableRS));
			oneOf(tableRS).next(); inSequence(tableRSSeq); will(returnValue(true));
			oneOf(tableRS).close(); inSequence(tableRSSeq);
			oneOf(stm).close();
			oneOf(con).close();
		}});
		cache.setName("books");
		cache.setDataSource(dataSource);
		cache.afterPropertiesSet();
	}
}