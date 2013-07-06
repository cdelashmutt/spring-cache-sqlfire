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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.junit.After;
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
 * Test out the simple string cache
 * 
 * @author cdelashmutt
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class JSONRepositoryTests
{
	Logger log = LoggerFactory.getLogger(JSONRepositoryTests.class);

	@Autowired
	private CacheManager manager;

	@Autowired
	private JSONService jsonService;

	@After
	public void clearCache()
	{
		manager.getCache("json").clear();
	}

	@Test
	public void testManagerCreated()
		throws Exception
	{
		assertNotNull(manager);
	}

	@Test
	public void testSave()
	{
		StopWatch sw = new StopWatch("testSave");
		sw.start();
		jsonService.save("1", "foobar");
		sw.stop();
		log.info(sw.prettyPrint());
	}

	@Test
	public void testDelete()
	{
		Book book = new Book(1, "Lord of the Rings");
		jsonService.save(String.valueOf(book.getId()), book.toString());
		jsonService.delete(String.valueOf(book.getId()));
	}

	@Test
	public void testGetById()
	{
		Book book = new Book(1, "Lord of the Rings");
		jsonService.save(String.valueOf(book.getId()), book.toString());
		String book2 = jsonService.getById(String.valueOf(book.getId()));
		assertThat(book.toString(), equalTo(book2));
	}

	@Test
	public void testCache()
	{
		Book book = new Book(1, "Lord of the Rings");
		jsonService.save(String.valueOf(book.getId()), book.toString());
		jsonService.evict(String.valueOf(book.getId()));
		StopWatch sw = new StopWatch("testCache");
		sw.start("uncached");
		jsonService.getById(String.valueOf(book.getId()));
		sw.stop();
		sw.start("cached");
		jsonService.getById(String.valueOf(book.getId()));
		sw.stop();
		log.info(sw.prettyPrint());
	}

	@Test
	public void testCacheEvict()
	{
		Book book = new Book(1, "Lord of the Rings");
		jsonService.save(String.valueOf(book.getId()), book.toString());
		StopWatch sw = new StopWatch("testCacheEvict");
		sw.start("before delete");
		jsonService.getById(String.valueOf(book.getId()));
		sw.stop();
		jsonService.delete(String.valueOf(book.getId()));
		sw.start("after delete");
		String book2 = jsonService.getById(String.valueOf(book.getId()));
		sw.stop();
		assertThat(book2, nullValue());
		log.info(sw.prettyPrint());
	}


	@Test
	public void testCacheUpdate()
	{
		Book book = new Book(1, "Lord of the Rings");
		jsonService.save(String.valueOf(book.getId()), book.toString());
		book.setTitle("The Hobbit");
		jsonService.save(String.valueOf(book.getId()), book.toString());
		String book2 = jsonService.getById(String.valueOf(book.getId()));
		assertThat(book2, equalTo(book2.toString()));
	}

}
