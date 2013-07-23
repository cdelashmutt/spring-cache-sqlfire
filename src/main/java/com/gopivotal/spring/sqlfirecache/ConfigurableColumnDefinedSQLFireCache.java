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
import java.util.List;

/**
 * A simple, column oriented cache intended to be configured via dependency injection.
 *
 * @author cdelashmutt
 */
public class ConfigurableColumnDefinedSQLFireCache
	extends AbstractColumnDefinedSQLFireCache
{
	
	List<ColumnDefinition> dataColumns = new ArrayList<ColumnDefinition>();

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.AbstractColumnDefinedSQLFireCache#getDataColumns()
	 */
	@Override
	protected List<ColumnDefinition> getDataColumns()
	{
		// TODO Auto-generated method stub
		return dataColumns;
	}

	/**
	 * @param dataColumns the dataColumns to set
	 */
	public void setDataColumns(List<ColumnDefinition> dataColumns)
	{
		this.dataColumns = dataColumns;
	}

}
