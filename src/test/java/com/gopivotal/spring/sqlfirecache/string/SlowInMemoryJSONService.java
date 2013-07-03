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
package com.gopivotal.spring.sqlfirecache.string;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteHashMap;

/**
 * An in memory JSON producing service that simulates slow conversions.
 *
 * @author cdelashmutt
 */
@Service
public class SlowInMemoryJSONService
implements JSONService
{

	Map<String,String> json = new CopyOnWriteHashMap<String, String>();
	
	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.string.JSONService#getById(java.lang.String)
	 */
	@Override
	public String getById(String id)
	{
		try
		{
			Thread.sleep(5);
		}
		catch (InterruptedException e)
		{
		}
		return json.get(id);
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.string.JSONService#delete(java.lang.String)
	 */
	@Override
	public void delete(String id)
	{
		json.remove(id);
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.string.JSONService#save(java.lang.String)
	 */
	@Override
	public String save(String id, String entity)
	{
		json.put(id, entity);
		return entity;
	}

	/* (non-Javadoc)
	 * @see com.gopivotal.spring.sqlfirecache.string.JSONService#evict(java.lang.String)
	 */
	@Override
	public void evict(String id)
	{
		//Noop, just for cache testing
	}

}
