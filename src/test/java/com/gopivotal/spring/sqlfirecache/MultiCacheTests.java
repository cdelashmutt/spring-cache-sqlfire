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

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import com.gopivotal.spring.sqlfirecache.serialized.Book;
import com.gopivotal.spring.sqlfirecache.string.JSONService;

/**
 * Try loading multiple caches.
 * 
 * @author cdelashmutt
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class MultiCacheTests
{
	Logger log = LoggerFactory.getLogger(JSONRepositoryTests.class);

	@Autowired
	private CacheManager manager;

	@Autowired
	private Repository<Book, Integer> bookRepository;

	@Autowired
	private JSONService jsonService;

	@Test
	public void testManagerCreated()
		throws Exception
	{
		assertNotNull(manager);
	}
	
	@Test
	public void testStringCache()
	{
		Book book = new Book(1, "Lord of the Rings");
		jsonService.save(String.valueOf(book.getId()), book.toString());
		jsonService.evict(String.valueOf(book.getId()));
		StopWatch sw = new StopWatch("testStringCache");
		sw.start("uncached");
		jsonService.getById(String.valueOf(book.getId()));
		sw.stop();
		sw.start("cached");
		jsonService.getById(String.valueOf(book.getId()));
		sw.stop();
		log.info(sw.prettyPrint());
	}

	@Test
	public void testSerializedCache()
	{
		Book book = new Book(null, "Lord of the Rings");
		book = bookRepository.save(book);
		StopWatch sw = new StopWatch("testSerializedCache");
		sw.start("uncached");
		bookRepository.getById(book.getId());
		sw.stop();
		sw.start("cached");
		bookRepository.getById(book.getId());
		sw.stop();
		log.info(sw.prettyPrint());
	}

}
