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

import org.springframework.util.Assert;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;

/**
 * Prepends bean property names with the passed in string.
 *
 * @author cdelashmutt
 */
public class PrependedBeanPropertySqlParameterSource
	extends BeanPropertySqlParameterSource
{

	private String prepend;
	
	/**
	 * Constructs a BeanPropertySqlParameterSource that transforms the property names
	 *
	 * @param object
	 */
	public PrependedBeanPropertySqlParameterSource(Object object, String prepend)
	{
		super(object);
		this.prepend = prepend;
		Assert.notNull(prepend, "Must provide a non-null prepend");

		Assert.isTrue(prepend.length() > 0, "Must provide a prepend greater than 0 length");
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource#getReadablePropertyNames()
	 */
	@Override
	public String[] getReadablePropertyNames()
	{
		String props[] = super.getReadablePropertyNames(); 
		for(int i = 0; i < props.length; i++)
			props[i] = prepend + props[i];
		return props;
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource#hasValue(java.lang.String)
	 */
	@Override
	public boolean hasValue(String paramName)
	{
		return super.hasValue(removePrepend(paramName));
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource#getValue(java.lang.String)
	 */
	@Override
	public Object getValue(String paramName)
		throws IllegalArgumentException
	{
		return super.getValue(removePrepend(paramName));
	}

	/* (non-Javadoc)
	 * @see org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource#getSqlType(java.lang.String)
	 */
	@Override
	public int getSqlType(String paramName)
	{
		// TODO Auto-generated method stub
		return super.getSqlType(removePrepend(paramName));
	}

	private String removePrepend(String paramName)
	{
		if(paramName.startsWith(prepend))
			return paramName.substring(prepend.length());
		else
			return paramName;
	}
}
