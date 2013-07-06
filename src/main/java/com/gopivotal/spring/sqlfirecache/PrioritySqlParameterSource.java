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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * A prioritized SqlParameterSource that maintains a list of SqlParameterSources, and checks all in turn for each method call.
 *
 * @author cdelashmutt
 */
public class PrioritySqlParameterSource
	implements SqlParameterSource
{

	List<SqlParameterSource> sources = new ArrayList<SqlParameterSource>();
	
	/**
	 * Builds a composite parameter source using the given sources in the order they are passed.
	 *
	 */
	public PrioritySqlParameterSource(SqlParameterSource... parameterSources)
	{
		Collections.addAll(sources, parameterSources);
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.SqlParameterSource#hasValue(java.lang.String)
	 */
	@Override
	public boolean hasValue(String paramName)
	{
		boolean value = false;
		for(SqlParameterSource source : sources)
		{
			value = source.hasValue(paramName);
			if(value == true)
			{
				break;
			}
		}
		return value;
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.SqlParameterSource#getValue(java.lang.String)
	 */
	@Override
	public Object getValue(String paramName)
		throws IllegalArgumentException
	{
		for(SqlParameterSource source : sources)
		{
			if(source.hasValue(paramName)) return source.getValue(paramName);
		}
		//We get here only if no enclosed sources have the value.
		throw new IllegalArgumentException("No value registered for key '" + paramName + "'");
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.SqlParameterSource#getSqlType(java.lang.String)
	 */
	@Override
	public int getSqlType(String paramName)
	{
		for(SqlParameterSource source : sources)
		{
			if(source.hasValue(paramName)) return source.getSqlType(paramName);
		}
		return SqlParameterSource.TYPE_UNKNOWN;
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.SqlParameterSource#getTypeName(java.lang.String)
	 */
	@Override
	public String getTypeName(String paramName)
	{
		for(SqlParameterSource source : sources)
		{
			if(source.hasValue(paramName)) return source.getTypeName(paramName);
		}
		return null;
	}

}
