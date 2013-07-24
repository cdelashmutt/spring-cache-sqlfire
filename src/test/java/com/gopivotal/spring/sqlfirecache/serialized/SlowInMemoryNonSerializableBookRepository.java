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
package com.gopivotal.spring.sqlfirecache.serialized;

import java.util.Map;

import org.springframework.stereotype.Service;

import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteHashMap;
import com.gopivotal.spring.sqlfirecache.Repository;

/**
 * A simulated slow repository for books.
 *
 * @author cdelashmutt
 */
@Service("nonSerializableBookRepository")
public class SlowInMemoryNonSerializableBookRepository
	implements Repository<NonSerializableBook, Integer>
{
	Map<Integer,NonSerializableBook> books = new CopyOnWriteHashMap<Integer,NonSerializableBook>();
	int nextId = 1;
	
	public NonSerializableBook save(NonSerializableBook book)
	{
		if(book.getId() == null)
		{
			synchronized(this)
			{
				book.setId(nextId++);
			}
		}
		books.put(book.getId(),book);
		return book;
	}
	
	public void delete(NonSerializableBook book)
	{
		books.remove(book.getId());
	}
	
	public NonSerializableBook getById(Integer id)
	{
		try
		{
			Thread.sleep(5);
		}
		catch (InterruptedException e)
		{
			//Ignore
		}
		return books.get(id);
	}
}
